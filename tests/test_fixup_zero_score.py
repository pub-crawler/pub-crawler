"""Tests for fixup_zero_score -- the one-shot migration that rewrites every
queue member's score to the constant 0 the pure-priority dispatcher enqueues
at, so the member prefix (depth|type|ts) becomes the total queue order for the
existing backlog, not just for newly enqueued jobs.

Contract:
  - Every member's score becomes 0; the member strings themselves are
    byte-for-byte unchanged (prefix, ts, and job JSON all survive).
  - No members are added or removed -- the queue's member set is preserved.
  - Idempotent: members already at score 0 stay put; a second run is a no-op.
  - An empty queue is a no-op (returns 0, no error).
  - Returns the number of members processed (== queue size when writes don't
    overlap the scan; see the batching test).
  - Run with the crawler STOPPED (it rewrites the queue while iterating).

Black-box: written from the contract above; uses fakeredis and inspects the
queue ZSET directly. Members are built in the dispatcher's real
`depth|type|ts|job` format so the ordering assertion means what it says.

Assumptions to flag if they differ: the function name, the (r, batch_size=...)
signature, and the exact-count return value.
"""

import json

from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import QUEUE
from fixup_zero_score import fixup_zero_score

TS = "2026-07-15T12:00:00.000000Z"


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def member(depth_code, type_code, job, ts=TS):
    """A queue member in the dispatcher's `depth|type|ts|job` format."""
    return f"{depth_code}|{type_code}|{ts}|{json.dumps(job, sort_keys=True)}"


def page(tag, depth=2):
    return {
        "job_type": "page",
        "page_id": f"https://x.example/users/{tag}/followers?page=1",
        "depth": depth,
    }


async def queue(r):
    """{member_str: score} for the whole queue."""
    return {m.decode(): s for m, s in await r.zrange(QUEUE, 0, -1, withscores=True)}


# ---------------------------------------------------------------------------
# Re-scoring: every score becomes 0, members untouched
# ---------------------------------------------------------------------------


async def test_rescores_a_timestamp_scored_member_to_zero():
    r = fake_redis()
    m = member("02", "40", page("a"))
    await r.zadd(QUEUE, {m: 1784120355000})

    count = await fixup_zero_score(r)

    assert count == 1
    assert await queue(r) == {m: 0}


async def test_member_strings_survive_byte_for_byte():
    # Only scores change: prefix, ts, and job JSON are not rewritten.
    r = fake_redis()
    members = {
        member("00", "40", page("seed", depth=0)): 1784120000000,
        member(
            "01",
            "30",
            {
                "job_type": "collection",
                "collection_id": "https://x.example/c",
                "depth": 1,
            },
        ): 1784120001000,
        member(
            "03",
            "20",
            {"job_type": "actor", "actor_id": "https://x.example/users/z", "depth": 3},
        ): 1784120002000,
    }
    await r.zadd(QUEUE, members)

    await fixup_zero_score(r)

    assert set(await queue(r)) == set(members)  # same member set, nothing added or lost


async def test_all_scores_zero_after_the_run():
    r = fake_redis()
    await r.zadd(
        QUEUE,
        {member("02", "40", page(f"u{i}")): 1784120000000 + i for i in range(7)},
    )

    await fixup_zero_score(r)

    assert all(score == 0 for score in (await queue(r)).values())


async def test_prefix_order_becomes_the_queue_order():
    # The point of the fixup: with scores flattened, zrange order IS the
    # depth|type|ts prefix order -- a shallow page scored LATE now sorts ahead
    # of a deep page scored EARLY.
    r = fake_redis()
    deep_early = member("03", "40", page("deep", depth=3))
    shallow_late = member("00", "40", page("seed", depth=0))
    await r.zadd(QUEUE, {deep_early: 100, shallow_late: 200})

    await fixup_zero_score(r)

    order = [m.decode() for m in await r.zrange(QUEUE, 0, -1)]
    assert order == [shallow_late, deep_early]


# ---------------------------------------------------------------------------
# Idempotence + edges
# ---------------------------------------------------------------------------


async def test_second_run_is_a_noop():
    r = fake_redis()
    m = member("01", "40", page("a", depth=1))
    await r.zadd(QUEUE, {m: 1784120355000})

    await fixup_zero_score(r)
    first = await queue(r)
    await fixup_zero_score(r)

    assert await queue(r) == first == {m: 0}


async def test_empty_queue_is_a_noop():
    r = fake_redis()

    count = await fixup_zero_score(r)

    assert count == 0
    assert await queue(r) == {}


async def test_batching_covers_every_member():
    # More members than one write batch: both the mid-scan flush and the final
    # partial-batch flush must run. End state is what matters; the processed
    # count may exceed the queue size if the scan re-delivers rewritten
    # members, so it's pinned as a lower bound here.
    r = fake_redis()
    members = {member("02", "40", page(f"u{i}")): 1784120000000 + i for i in range(5)}
    await r.zadd(QUEUE, members)

    count = await fixup_zero_score(r, batch_size=2)

    assert count >= 5
    q = await queue(r)
    assert set(q) == set(members)
    assert all(score == 0 for score in q.values())
