"""S6: the wiki RAG branch, in the `rag` database on RDS (pgvector), on a daily schedule.

  wiki_pages --> wiki_chunks --> wiki_embeddings --(every_chunk_has_a_vector, blocking)

- wiki_pages: the published pages, fetched with the ETag stored last time: an unchanged page costs a 304
  and no body. Pages no longer published are deleted with their chunks.
- wiki_chunks: the given chunker, labpipes.chunking.chunk_markdown(SIMPLE_MAX_TOKENS, SIMPLE_OVERLAP)
  (decision 015). LLM chunking (the chat model picks sentence split points) is kept but switched off
  (CHUNKER = "llm" turns it back on): on 2026-11-22 it cut the 16 pages into 83 one-sentence chunks.
  Each page remembers which chunker and settings made its chunks (CHUNKING_VERSION), so changing them
  re-chunks every page on the next run. A changed page's old chunks (and vectors) are deleted; ids are
  Chunk.content_hash, so unchanged text keeps its id.
- wiki_embeddings: embeds only chunks without a vector (nomic-embed-text, 768 dimensions, "search_document:"
  added by the `embed` resource), stored as pgvector vector(768) with an HNSW cosine index (decision 016).
- every_chunk_has_a_vector: blocking check. ingest_wiki: the job (scripts/dagster-run.sh ingest_wiki);
  the schedule runs it daily at 06:00 UTC.

The student's decisions live in the block below; changing one changes a graded decision.
"""

import json
import re

import dagster as dg

from ..chunking import TOKENS_PER_WORD, Chunk, _sections, chunk_markdown, estimate_tokens
from ..resources import LlmChat, LlmEmbed, RagStore, WikiApi

# ======================================================================================================
# THE STUDENT'S DECISIONS (decisions 015 and 016; schedule chosen on 2026-11-22).
CHUNKER = "simple"  # "simple": chunk_markdown below; "llm": LLM split points (kept, switched off)
SIMPLE_MAX_TOKENS = 256  # one chunk per page on today's wiki (largest page 181 tokens)
SIMPLE_OVERLAP = 0
# LLM chunking, used only when CHUNKER == "llm":
MAX_TOKENS = 128  # largest chunk the LLM may make (body only, without the title/heading line)
FALLBACK_OVERLAP = 32  # chunk_markdown overlap when the LLM's answer is unusable
LLM_TEMPERATURE = 0.0  # same page -> same split, so re-runs keep chunk ids
INDEX = "hnsw"  # decision 016: HNSW, cosine distance
SCHEDULE = "0 6 * * *"  # daily at 06:00 UTC
# ======================================================================================================


def chunking_version() -> str:
    """Which chunker and settings make chunks now. A page chunked with another version is re-chunked."""
    if CHUNKER == "llm":
        return f"llm:{MAX_TOKENS}:{FALLBACK_OVERLAP}:t{LLM_TEMPERATURE:g}"
    return f"simple:{SIMPLE_MAX_TOKENS}:{SIMPLE_OVERLAP}"


DIMENSIONS = 768
_SENTENCE_END = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(\[*_`-])")

SCHEMA = [
    "create extension if not exists vector",
    "create schema if not exists rag",
    """create table if not exists rag.pages (
         id text primary key, title text not null, etag text not null, updated_at text not null,
         body text not null, chunked_etag text, fetched_at timestamptz not null default now())""",
    f"""create table if not exists rag.chunks (
         id text primary key, page_id text not null references rag.pages(id) on delete cascade,
         idx int not null, heading text not null, content text not null, tokens int not null,
         method text not null, embedding vector({DIMENSIONS}))""",
    # Added after the first deploy: which chunker made a page's chunks, and 'simple' as a method.
    "alter table rag.pages add column if not exists chunked_with text",
    "alter table rag.chunks drop constraint if exists chunks_method_check",
    "alter table rag.chunks add constraint chunks_method_check check (method in ('simple', 'llm', 'fallback'))",
    "create index if not exists chunks_page_id on rag.chunks (page_id)",
    f"create index if not exists chunks_embedding_{INDEX} on rag.chunks using {INDEX} (embedding vector_cosine_ops)",
]


# ---- LLM chunking: plain functions -------------------------------------------------------------------


def sentences(body: str) -> list[str]:
    """A section's text as sentences. Paragraph and list-item breaks always end a sentence."""
    out = []
    for block in re.split(r"\n\s*\n|\n(?=\s*(?:[-*+]|\d+[.)])\s)", body):
        block = " ".join(block.split())
        if block:
            out.extend(s.strip() for s in _SENTENCE_END.split(block) if s.strip())
    return out


def numbered(title: str, markdown: str) -> list[tuple[str, str]]:
    """(heading, sentence) for every sentence of the page, in order."""
    return [(heading, s) for heading, body in _sections(markdown) for s in sentences(body)]


SYSTEM = (
    "You split documents into chunks for search. You never rewrite text. You answer with JSON only: "
    '{"starts": [list of sentence numbers]} where each number is a sentence that begins a new chunk.'
)


def split_prompt(title: str, items: list[tuple[str, str]], max_tokens: int) -> list[dict]:
    """The messages asking for split points. Sections are shown so the model sees the topics; a new
    section always starts a chunk anyway."""
    lines, last = [], None
    for i, (heading, s) in enumerate(items, 1):
        if heading != last:
            lines.append(f"## {heading or title}")
            last = heading
        lines.append(f"[{i}] {s}")
    max_words = int(max_tokens / TOKENS_PER_WORD)
    user = (
        f"Document: {title}\n\n" + "\n".join(lines) + "\n\n"
        f"Group consecutive sentences about the same rule or topic into one chunk. Each chunk must stay under "
        f"{max_words} words. Sentence 1 always starts a chunk. Answer with the JSON only."
    )
    return [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]


def parse_starts(reply: str, n: int) -> list[int] | None:
    """The sentence numbers in the model's reply, or None if it isn't usable."""
    m = re.search(r"\{.*\}", reply, re.S)
    try:
        starts = json.loads(m.group(0))["starts"] if m else None
    except (ValueError, KeyError, TypeError):
        return None
    if not isinstance(starts, list) or not all(isinstance(x, int) for x in starts):
        return None
    starts = sorted(set(starts))
    if not starts or starts[0] != 1 or starts[-1] > n:
        return None
    return starts


def chunks_from_starts(title: str, items: list[tuple[str, str]], starts: list[int], max_tokens: int) -> list[Chunk] | None:
    """Chunks of the page's exact sentences, cut at `starts` and at every new section. None if any chunk
    is over max_tokens (the caller then falls back)."""
    cuts = {s - 1 for s in starts} | {i for i in range(1, len(items)) if items[i][0] != items[i - 1][0]} | {0}
    out: list[Chunk] = []
    current: list[str] = []
    for i, (_heading, s) in enumerate(items):
        if i in cuts and current:
            out.append(_make(title, items[i - 1][0], current, len(out)))
            current = []
        current.append(s)
    if current:
        out.append(_make(title, items[-1][0], current, len(out)))
    if any(estimate_tokens(c.text.split("\n\n", 1)[1]) > max_tokens for c in out):
        return None
    return out


def _make(title: str, heading: str, parts: list[str], index: int) -> Chunk:
    prefix = f"{title} > {heading}" if heading else title
    text = f"{prefix}\n\n" + " ".join(parts)
    return Chunk(index, heading, text, estimate_tokens(text))


def chunk_page(chat: LlmChat, title: str, markdown: str) -> tuple[list[Chunk], str, str]:
    """(chunks, method, why): the LLM's split points when usable, otherwise chunk_markdown."""
    items = numbered(title, markdown)
    if not items:
        return [], "llm", "empty page"
    if len(items) == 1:
        one = chunks_from_starts(title, items, [1], MAX_TOKENS)
        if one is not None:
            return one, "llm", "one sentence"
    try:
        reply = chat.complete(split_prompt(title, items, MAX_TOKENS), temperature=LLM_TEMPERATURE, max_tokens=200)
    except Exception as e:  # LLM host down or slow: the page still gets chunks
        return chunk_markdown(title, markdown, MAX_TOKENS, FALLBACK_OVERLAP), "fallback", f"LLM call failed: {type(e).__name__}"
    starts = parse_starts(reply, len(items))
    if starts is None:
        return chunk_markdown(title, markdown, MAX_TOKENS, FALLBACK_OVERLAP), "fallback", f"unusable reply: {reply[:80]!r}"
    out = chunks_from_starts(title, items, starts, MAX_TOKENS)
    if out is None:
        return chunk_markdown(title, markdown, MAX_TOKENS, FALLBACK_OVERLAP), "fallback", f"a chunk over {MAX_TOKENS} tokens"
    return out, "llm", ""


def vector_literal(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.7g}" for x in v) + "]"


# ---- the rag database --------------------------------------------------------------------------------


class RagDb:
    """The two tables, through a DB-API connection (psycopg2 from RagStore.connect). Each method commits."""

    def __init__(self, conn):
        self.conn = conn

    def _run(self, sql: str, params=None, fetch: bool = False):
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if fetch else None
        self.conn.commit()
        return rows

    def ensure_schema(self) -> None:
        for sql in SCHEMA:
            self._run(sql)

    def etags(self) -> dict[str, str]:
        return dict(self._run("select id, etag from rag.pages", fetch=True))

    def upsert_page(self, page) -> None:
        self._run(
            """insert into rag.pages (id, title, etag, updated_at, body) values (%s, %s, %s, %s, %s)
               on conflict (id) do update set title = excluded.title, etag = excluded.etag,
                 updated_at = excluded.updated_at, body = excluded.body, fetched_at = now()""",
            (page.id, page.title, page.etag, page.updated_at, page.body))

    def delete_pages(self, ids: list[str]) -> None:
        if ids:
            self._run("delete from rag.pages where id = any(%s)", (ids,))  # chunks go with them (cascade)

    def pages_to_chunk(self, version: str) -> list[tuple[str, str, str, str]]:
        """(id, title, body, etag) of pages not yet chunked in their current version, or chunked by
        another chunker or settings than `version`."""
        return self._run("select id, title, body, etag from rag.pages where chunked_etag is distinct from etag "
                         "or chunked_with is distinct from %s order by id", (version,), fetch=True)

    def replace_chunks(self, page_id: str, etag: str, chunks: list[Chunk], method: str, version: str) -> tuple[int, int]:
        """The page's chunks become exactly `chunks`; a chunk whose id already exists keeps its vector.
        Returns (kept, added). One transaction, so a page is never half-chunked."""
        ids = [c.content_hash(page_id) for c in chunks]
        with self.conn.cursor() as cur:
            cur.execute("select id from rag.chunks where page_id = %s", (page_id,))
            existing = {r[0] for r in cur.fetchall()}
            cur.execute("delete from rag.chunks where page_id = %s and not (id = any(%s))", (page_id, ids))
            added = 0
            for cid, c in zip(ids, chunks, strict=True):
                if cid in existing:
                    cur.execute("update rag.chunks set idx = %s, heading = %s, method = %s where id = %s", (c.index, c.heading, method, cid))
                else:
                    cur.execute("insert into rag.chunks (id, page_id, idx, heading, content, tokens, method) values (%s, %s, %s, %s, %s, %s, %s)",
                                (cid, page_id, c.index, c.heading, c.text, c.tokens, method))
                    added += 1
            cur.execute("update rag.pages set chunked_etag = %s, chunked_with = %s where id = %s", (etag, version, page_id))
        self.conn.commit()
        return len(existing & set(ids)), added

    def chunks_without_vector(self) -> list[tuple[str, str]]:
        return self._run("select id, content from rag.chunks where embedding is null order by page_id, idx", fetch=True)

    def set_vector(self, chunk_id: str, vector: list[float]) -> None:
        self._run("update rag.chunks set embedding = %s::vector where id = %s", (vector_literal(vector), chunk_id))

    def counts(self) -> dict[str, int]:
        (pages, chunks, missing, simple, llm, fallback, empty), = self._run(
            """select (select count(*) from rag.pages), count(*), count(*) filter (where embedding is null),
                      count(*) filter (where method = 'simple'), count(*) filter (where method = 'llm'),
                      count(*) filter (where method = 'fallback'),
                      (select count(*) from rag.pages p where not exists (select 1 from rag.chunks c where c.page_id = p.id))
               from rag.chunks""", fetch=True)
        return {"pages": pages, "chunks": chunks, "without_vector": missing, "simple_chunks": simple, "llm_chunks": llm,
                "fallback_chunks": fallback,
                "pages_without_chunks": empty}


def open_db(rag: RagStore) -> RagDb:
    """The rag database, schema ensured. Tests replace this with an in-memory fake."""
    db = RagDb(rag.connect())
    db.ensure_schema()
    return db


# ---- Dagster -----------------------------------------------------------------------------------------


@dg.asset(group_name="wiki", kinds={"postgres"})
def wiki_pages(context: dg.AssetExecutionContext, wiki: WikiApi, rag: RagStore) -> dg.MaterializeResult:
    """The wiki's published pages in rag.pages, fetched only when their ETag changed. Pages no longer
    published are deleted, with their chunks."""
    client, db = wiki.client(), open_db(rag)
    stored = db.etags()
    refs = client.pages()
    fetched, unchanged = [], 0
    for ref in refs:
        page = client.page(ref.id, stored.get(ref.id))
        if page is None:
            unchanged += 1
            continue
        db.upsert_page(page)
        fetched.append(ref.id)
    removed = sorted(set(stored) - {r.id for r in refs})
    db.delete_pages(removed)
    context.log.info(f"{len(refs)} published: {len(fetched)} fetched, {unchanged} unchanged (304), {len(removed)} removed")
    return dg.MaterializeResult(metadata={
        "published": len(refs), "pages_fetched": len(fetched), "pages_unchanged": unchanged, "pages_removed": len(removed),
        "fetched": ", ".join(fetched) or "none", "removed": ", ".join(removed) or "none",
    })


@dg.asset(deps=[wiki_pages], group_name="wiki", kinds={"postgres"})
def wiki_chunks(context: dg.AssetExecutionContext, rag: RagStore, chat: LlmChat) -> dg.MaterializeResult:
    """Chunks of every page not yet chunked in its current version or with the current chunker settings
    (chunk_markdown by default; LLM split points when CHUNKER == "llm"). Old chunks are deleted."""
    db, version = open_db(rag), chunking_version()
    todo = db.pages_to_chunk(version)
    added = kept = fallbacks = 0
    for page_id, title, body, etag in todo:
        if CHUNKER == "llm":
            chunks, method, why = chunk_page(chat, title, body)
        else:
            chunks, method, why = chunk_markdown(title, body, SIMPLE_MAX_TOKENS, SIMPLE_OVERLAP), "simple", ""
        k, a = db.replace_chunks(page_id, etag, chunks, method, version)
        kept, added = kept + k, added + a
        if method == "fallback":
            fallbacks += 1
            context.log.warning(f"{page_id}: fallback to chunk_markdown ({why})")
        context.log.info(f"{page_id}: {len(chunks)} chunks by {method} ({a} new)")
    counts = db.counts()
    return dg.MaterializeResult(metadata={
        "chunking": version, "pages_chunked": len(todo), "chunks_added": added, "chunks_kept": kept, "pages_fallback": fallbacks,
        "chunks_total": counts["chunks"], "simple_chunks": counts["simple_chunks"], "llm_chunks": counts["llm_chunks"],
        "fallback_chunks": counts["fallback_chunks"],
    })


@dg.asset(deps=[wiki_chunks], group_name="wiki", kinds={"postgres"})
def wiki_embeddings(context: dg.AssetExecutionContext, rag: RagStore, embed: LlmEmbed) -> dg.MaterializeResult:
    """Vectors for the chunks that have none yet. Unchanged chunks are never embedded again."""
    db = open_db(rag)
    todo = db.chunks_without_vector()
    if todo:
        vectors = embed.embed_documents([content for _, content in todo])
        # strict=False on purpose: if the embedder returns fewer vectors, those chunks stay without one
        # and the blocking every_chunk_has_a_vector check stops the run.
        for (chunk_id, _), v in zip(todo, vectors, strict=False):
            db.set_vector(chunk_id, v)
    counts = db.counts()
    context.log.info(f"embedded {len(todo)} chunks; {counts['chunks']} chunks in total")
    return dg.MaterializeResult(metadata={"chunks_embedded": len(todo), "chunks_total": counts["chunks"],
                                          "without_vector": counts["without_vector"]})


@dg.asset_check(asset=wiki_embeddings, blocking=True, description="Every chunk in rag.chunks has a vector.")
def every_chunk_has_a_vector(rag: RagStore) -> dg.AssetCheckResult:
    counts = open_db(rag).counts()
    passed = counts["chunks"] > 0 and counts["without_vector"] == 0
    return dg.AssetCheckResult(
        passed=passed, severity=dg.AssetCheckSeverity.ERROR,
        description=f"{counts['without_vector']} of {counts['chunks']} chunks have no vector",
        metadata={k: dg.MetadataValue.int(v) for k, v in counts.items()},
    )


ingest_wiki = dg.define_asset_job(
    "ingest_wiki",
    selection=dg.AssetSelection.assets(wiki_pages, wiki_chunks, wiki_embeddings),
    description="Fetch changed wiki pages (ETags), chunk them, embed new chunks; blocking: every chunk has a vector.",
)

ingest_wiki_daily = dg.ScheduleDefinition(
    name="ingest_wiki_daily", job=ingest_wiki, cron_schedule=SCHEDULE, execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
