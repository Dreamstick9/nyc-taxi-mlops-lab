"""S6 ingest: LLM chunking (split points, validation, fallback) and the three assets against in-memory
fakes for the wiki, the chat and embedding servers, and the rag database. Nothing here calls AWS."""

from __future__ import annotations

import json

import dagster as dg
import pytest

from labpipes.chunking import estimate_tokens
from labpipes.defs import rag
from labpipes.defs.rag import (
    MAX_TOKENS,
    chunk_page,
    chunks_from_starts,
    every_chunk_has_a_vector,
    ingest_wiki,
    ingest_wiki_daily,
    numbered,
    parse_starts,
    sentences,
    wiki_chunks,
    wiki_embeddings,
    wiki_pages,
)
from labpipes.resources import LlmChat, LlmEmbed, RagStore, WikiApi
from labpipes.wiki import Page, PageRef

AIRPORT = """# Airport pickup buffer (OPS-2291)

Quotes for trips starting at JFK or LaGuardia add a pickup buffer. The buffer is 12 minutes at JFK.
It is 8 minutes at LaGuardia.

Drivers cannot shorten the buffer. Riders see it as "airport pickup" on the quote.
"""


# ---- plain functions ----------------------------------------------------------------------------------


def test_sentences_split_on_ends_and_paragraphs():
    assert sentences("One rule. Two rules!\n\nThird part? Yes") == ["One rule.", "Two rules!", "Third part?", "Yes"]
    assert sentences("- item one\n- item two") == ["- item one", "- item two"]


def test_parse_starts_accepts_json_and_refuses_junk():
    assert parse_starts('```json\n{"starts": [3, 1]}\n```', 5) == [1, 3]
    for bad in ["no json", '{"starts": []}', '{"starts": [2, 3]}', '{"starts": [1, 9]}', '{"starts": ["1"]}', '{"x": [1]}']:
        assert parse_starts(bad, 5) is None, bad


def test_chunks_keep_exact_words_and_cut_at_sections():
    md = "# A\n\nFirst. Second.\n\n## B\n\nThird. Fourth."
    items = numbered("Page", md)
    chunks = chunks_from_starts("Page", items, [1], MAX_TOKENS)
    # a new section always cuts; the heading path is chunk_markdown's ("A > B": B is under A)
    assert [c.text for c in chunks] == ["Page > A\n\nFirst. Second.", "Page > A > B\n\nThird. Fourth."]
    chunks = chunks_from_starts("Page", items, [1, 2, 4], MAX_TOKENS)
    assert [c.text.split("\n\n", 1)[1] for c in chunks] == ["First.", "Second.", "Third.", "Fourth."]
    assert [c.index for c in chunks] == [0, 1, 2, 3]


def test_a_chunk_over_the_limit_is_refused():
    long = " ".join(f"word{i}." for i in range(200))
    assert chunks_from_starts("Page", numbered("Page", long), [1], MAX_TOKENS) is None


class FakeChat(LlmChat):
    reply: str = '{"starts": [1, 3]}'
    fail: bool = False
    calls: list = []

    def complete(self, messages, temperature=0.0, max_tokens=256):
        self.calls.append({"messages": messages, "temperature": temperature})
        if self.fail:
            raise TimeoutError("slow")
        return self.reply


def test_chunk_page_uses_llm_split_points_at_temperature_0():
    chat = FakeChat(llm_host="x", calls=[])
    chunks, method, why = chunk_page(chat, "Airport pickup buffer", AIRPORT)
    assert method == "llm" and why == ""
    assert len(chunks) == 2 and "12 minutes at JFK" in chunks[0].text and "Drivers cannot shorten" in chunks[1].text
    assert chat.calls[0]["temperature"] == 0.0
    prompt = chat.calls[0]["messages"][1]["content"]
    assert "[1] Quotes for trips" in prompt and "[4] Drivers cannot shorten" in prompt
    # every word of the page is in exactly one chunk, unchanged
    body_words = " ".join(s for _, s in numbered("t", AIRPORT)).split()
    assert " ".join(c.text.split("\n\n", 1)[1] for c in chunks).split() == body_words


@pytest.mark.parametrize("chat", [
    FakeChat(llm_host="x", reply="I think chunk 1 and 3", calls=[]),
    FakeChat(llm_host="x", fail=True, calls=[]),
])
def test_unusable_llm_answer_falls_back_to_chunk_markdown(chat):
    chunks, method, why = chunk_page(chat, "Airport pickup buffer", AIRPORT)
    assert method == "fallback" and why
    assert chunks and all(estimate_tokens(c.text) <= MAX_TOKENS + 30 for c in chunks)


# ---- the assets, against in-memory fakes --------------------------------------------------------------


class FakeDb:
    """rag.pages and rag.chunks in dicts, with RagDb's methods."""

    def __init__(self):
        self.pages: dict[str, dict] = {}
        self.chunks: dict[str, dict] = {}

    def ensure_schema(self):
        pass

    def etags(self):
        return {k: p["etag"] for k, p in self.pages.items()}

    def upsert_page(self, page):
        old = self.pages.get(page.id, {})
        self.pages[page.id] = {"title": page.title, "etag": page.etag, "body": page.body, "chunked_etag": old.get("chunked_etag"),
                               "chunked_with": old.get("chunked_with")}

    def delete_pages(self, ids):
        for i in ids:
            self.pages.pop(i, None)
            for cid in [c for c, v in self.chunks.items() if v["page_id"] == i]:
                del self.chunks[cid]

    def pages_to_chunk(self, version):
        return [(k, p["title"], p["body"], p["etag"]) for k, p in sorted(self.pages.items())
                if p["chunked_etag"] != p["etag"] or p.get("chunked_with") != version]

    def replace_chunks(self, page_id, etag, chunks, method, version):
        ids = [c.content_hash(page_id) for c in chunks]
        existing = {c for c, v in self.chunks.items() if v["page_id"] == page_id}
        for cid in existing - set(ids):
            del self.chunks[cid]
        added = 0
        for cid, c in zip(ids, chunks, strict=True):
            if cid in self.chunks:
                self.chunks[cid]["method"] = method  # as RagDb: same text keeps its id and vector
            else:
                self.chunks[cid] = {"page_id": page_id, "content": c.text, "method": method, "embedding": None}
                added += 1
        self.pages[page_id]["chunked_etag"], self.pages[page_id]["chunked_with"] = etag, version
        return len(existing & set(ids)), added

    def chunks_without_vector(self):
        return [(c, v["content"]) for c, v in self.chunks.items() if v["embedding"] is None]

    def set_vector(self, chunk_id, vector):
        self.chunks[chunk_id]["embedding"] = vector

    def counts(self):
        return {"pages": len(self.pages), "chunks": len(self.chunks),
                "without_vector": sum(v["embedding"] is None for v in self.chunks.values()),
                "simple_chunks": sum(v["method"] == "simple" for v in self.chunks.values()),
                "llm_chunks": sum(v["method"] == "llm" for v in self.chunks.values()),
                "fallback_chunks": sum(v["method"] == "fallback" for v in self.chunks.values()),
                "pages_without_chunks": sum(not any(v["page_id"] == p for v in self.chunks.values()) for p in self.pages)}


class FakeWikiClient:
    def __init__(self, pages: dict[str, Page]):
        self.pages_by_id, self.gets = pages, []

    def pages(self):
        return [PageRef(p.id, p.title, p.updated_at, p.etag) for p in self.pages_by_id.values()]

    def page(self, page_id, etag=None):
        self.gets.append((page_id, etag))
        p = self.pages_by_id[page_id]
        return None if etag == p.etag else p


class FakeWiki(WikiApi):
    def client(self):
        return CLIENT


class FakeEmbed(LlmEmbed):
    def embed_documents(self, texts):
        EMBEDDED.extend(texts)
        return [[0.1] * 768 for _ in texts]


CLIENT: FakeWikiClient
EMBEDDED: list[str] = []


@pytest.fixture
def world(monkeypatch):
    global CLIENT
    db = FakeDb()
    monkeypatch.setattr(rag, "open_db", lambda _rag: db)
    CLIENT = FakeWikiClient({
        "airport-buffer": Page("airport-buffer", "Airport pickup buffer", "t1", '"a1"', AIRPORT),
        "quiet-hours": Page("quiet-hours", "Quiet hours", "t1", '"q1"', "# Quiet hours\n\nNo horns after 22:00."),
    })
    EMBEDDED.clear()
    return db


def run(**resources):
    return dg.materialize(
        [wiki_pages, wiki_chunks, wiki_embeddings, every_chunk_has_a_vector],
        resources={"wiki": FakeWiki(), "rag": RagStore(), "chat": FakeChat(llm_host="x", calls=[]), "embed": FakeEmbed(llm_host="x"), **resources},
    )


def meta(result, asset: str) -> dict:
    ev = result.asset_materializations_for_node(asset)[0]
    return {k: v.value for k, v in ev.metadata.items()}


def test_first_run_fetches_chunks_and_embeds_everything(world):
    r = run()
    assert r.success
    assert meta(r, "wiki_pages")["pages_fetched"] == 2 and meta(r, "wiki_pages")["pages_unchanged"] == 0
    assert meta(r, "wiki_chunks")["pages_chunked"] == 2
    assert meta(r, "wiki_chunks")["chunking"] == "simple:256:0"
    assert meta(r, "wiki_embeddings")["chunks_embedded"] == len(world.chunks) == 2  # one whole page each (simple 256/0)
    assert all(v["method"] == "simple" for v in world.chunks.values())
    checks = r.get_asset_check_evaluations()
    assert checks[0].passed


def test_second_run_fetches_nothing_and_embeds_nothing(world):
    run()
    r = run()
    assert meta(r, "wiki_pages")["pages_fetched"] == 0 and meta(r, "wiki_pages")["pages_unchanged"] == 2
    assert meta(r, "wiki_chunks")["pages_chunked"] == 0
    assert meta(r, "wiki_embeddings")["chunks_embedded"] == 0
    assert CLIENT.gets[-2:] == [("airport-buffer", '"a1"'), ("quiet-hours", '"q1"')]  # conditional GETs


def test_a_changed_page_replaces_its_old_chunks_and_a_removed_page_goes(world):
    run()
    old_quiet = {c for c, v in world.chunks.items() if v["page_id"] == "quiet-hours"}
    CLIENT.pages_by_id["quiet-hours"] = Page("quiet-hours", "Quiet hours", "t2", '"q2"', "# Quiet hours\n\nNo horns after 23:00.")
    del CLIENT.pages_by_id["airport-buffer"]
    r = run()
    assert meta(r, "wiki_pages")["pages_fetched"] == 1 and meta(r, "wiki_pages")["pages_removed"] == 1
    assert set(world.pages) == {"quiet-hours"}
    quiet = {c for c, v in world.chunks.items() if v["page_id"] == "quiet-hours"}
    assert quiet and not (quiet & old_quiet)  # the old version's chunks are gone
    assert all(v["embedding"] for v in world.chunks.values())
    assert meta(r, "wiki_embeddings")["chunks_embedded"] == 1


def test_llm_mode_splits_and_records_fallback(world, monkeypatch):
    monkeypatch.setattr(rag, "CHUNKER", "llm")
    r = run()
    assert meta(r, "wiki_chunks")["chunking"] == "llm:128:32:t0" and meta(r, "wiki_chunks")["llm_chunks"] == 3
    world.pages.clear(), world.chunks.clear()
    r = run(chat=FakeChat(llm_host="x", reply="no idea", calls=[]))
    assert meta(r, "wiki_chunks")["pages_fallback"] == 1  # quiet-hours is one sentence: no LLM call needed
    assert any(v["method"] == "fallback" for v in world.chunks.values())


def test_the_check_blocks_when_a_chunk_has_no_vector(world):
    class NoEmbed(LlmEmbed):
        def embed_documents(self, texts):
            return []

    r = dg.materialize(
        [wiki_pages, wiki_chunks, wiki_embeddings, every_chunk_has_a_vector],
        resources={"wiki": FakeWiki(), "rag": RagStore(), "chat": FakeChat(llm_host="x", calls=[]), "embed": NoEmbed(llm_host="x")},
        raise_on_error=False,
    )
    check = r.get_asset_check_evaluations()[0]
    assert not check.passed and every_chunk_has_a_vector.check_specs and next(iter(every_chunk_has_a_vector.check_specs)).blocking


def test_job_and_schedule():
    assert ingest_wiki.name == "ingest_wiki"
    assert ingest_wiki_daily.cron_schedule == "0 6 * * *" and ingest_wiki_daily.execution_timezone == "UTC"
    assert json.loads(json.dumps(rag.SCHEMA))[-1].startswith("create index if not exists chunks_embedding_hnsw")


def test_changing_the_chunker_rechunks_every_page(world, monkeypatch):
    run()
    simple_ids = set(world.chunks)
    monkeypatch.setattr(rag, "CHUNKER", "llm")
    r = run()
    assert meta(r, "wiki_pages")["pages_fetched"] == 0  # nothing new on the wiki
    assert meta(r, "wiki_chunks")["pages_chunked"] == 2  # but both pages are re-chunked with the new chunker
    assert len(world.chunks) == 3  # airport split in two by the LLM; quiet-hours (one sentence) keeps its id
    assert all(v["method"] == "llm" for v in world.chunks.values())
    monkeypatch.setattr(rag, "CHUNKER", "simple")
    r = run()
    assert meta(r, "wiki_chunks")["pages_chunked"] == 2 and set(world.chunks) == simple_ids  # same text -> same ids
    assert meta(r, "wiki_embeddings")["chunks_embedded"] == 1  # only airport changed text; quiet-hours kept its vector
