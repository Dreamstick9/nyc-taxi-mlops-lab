"""S6 step 4: retrieve (top-k by cosine distance from rag.chunks) and build_messages (fixed system prompt,
numbered excerpts with page ids in the user turn). The database is a fake; nothing here calls AWS."""

import asyncio

import rideapp.assistant as a


def chunk(page_id, content, score):
    return a.Chunk(id=f"id-{page_id}", page_id=page_id, title=page_id.replace("-", " ").title(), content=content, score=score)


def test_system_prompt_is_fixed_and_the_excerpts_go_in_the_user_turn():
    one = a.build_messages("Why is my airport quote longer?", [chunk("airport-buffer", "Airport pickup buffer\n\n12 minutes at JFK.", 0.71)])
    two = a.build_messages("What are quiet hours?", [chunk("quiet-hours", "No horns after 22:00.", 0.66)])
    assert one[0] == two[0] == {"role": "system", "content": a.SYSTEM_PROMPT}  # same prefix: llama.cpp caches it
    user = one[1]["content"]
    assert one[1]["role"] == "user" and user.startswith("Wiki excerpts:")
    assert "[1] airport-buffer (Airport Buffer):\nAirport pickup buffer\n\n12 minutes at JFK." in user
    assert user.endswith("Question: Why is my airport quote longer?")


def test_excerpts_are_numbered_in_retrieval_order():
    msgs = a.build_messages("q", [chunk("a-page", "A", 0.9), chunk("b-page", "B", 0.8)])
    user = msgs[1]["content"]
    assert user.index("[1] a-page") < user.index("[2] b-page")


def test_no_excerpts_keeps_the_same_rules():
    msgs = a.build_messages("q", [])
    assert msgs[0]["content"] == a.SYSTEM_PROMPT and "(no excerpts)" in msgs[1]["content"]


def test_the_prompt_forbids_invention_and_asks_for_citations():
    p = a.SYSTEM_PROMPT.lower()
    assert "only from the wiki excerpts" in p and "never guess or invent" in p and "cite the page ids" in p
    assert "contacts support" in p


class FakeCursor:
    def __init__(self, rows, seen):
        self.rows, self.seen = rows, seen

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        self.seen.append((sql, params))

    async def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows, seen):
        self.rows, self.seen = rows, seen

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor(self.rows, self.seen)


def test_retrieve_asks_for_top_k_by_cosine_distance(monkeypatch):
    seen = []
    rows = [("c1", "airport-buffer", "Airport pickup buffer", "text", 0.71), ("c2", "tlc-airports", "TLC airports", "t2", 0.55)]

    async def connect():
        return FakeConn(rows, seen)

    monkeypatch.setattr(a, "connect", connect)
    got = asyncio.run(a.retrieve([0.5] * 768))
    assert [c.page_id for c in got] == ["airport-buffer", "tlc-airports"] and got[0].score == 0.71
    sql, params = seen[0]
    assert "<=>" in sql and "order by c.embedding <=>" in sql and "limit %s" in sql
    assert params[2] == a.TOP_K == 2 and params[0] == params[1] and params[0].startswith("[0.5,")
