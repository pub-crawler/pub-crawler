"""Tests for fixup_seen(redis, graph) -- the one-off pre-crawl migration that
seeds the SEEN set from durable state and compacts the queue.

Contract (agreed):
  - Rebuilds SEEN from all four authoritative sources: queued jobs, in-flight
    jobs, failed jobs, and fetched actors (graph nodes carrying last_fetch_date).
    Bare graph nodes (no last_fetch_date) are NOT seeded -- they stay crawlable.
  - Compacts the queue in a single walk using SADD's return value as the gate:
    the first occurrence of a job_id is kept; any later duplicate, or any job
    already in-flight/failed/fetched, is ZREM'd.
  - Leaves malformed queue members (job_id is None) in place -- minimize
    destructiveness, they may carry useful info -- but never seeds a junk member.
  - Idempotent: a second run must not shrink the queue further or change SEEN
    (in particular it must not mistake the surviving de-duped jobs for dupes and
    wipe the queue) -- DEL SEEN + rebuild from authoritative state.

Run with the crawler STOPPED (DEL SEEN and the ZREMs assume no concurrent
writer). State is built through a real Dispatcher so the Redis encodings match
what fixup reads; assertions read back through Dispatcher.seen().

These are red until fixup_seen.py exists.
"""

import json

from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import QUEUE, SEEN, Dispatcher, iso_utc
from fixup_seen import add_queue_to_seen_and_dedupe, fixup_seen
from support import FakeGraph

A = "https://x.example/users/a"
B = "https://x.example/users/b"
C = "acct:c@x.example"  # a webfinger handle
D = "https://x.example/users/d"  # fetched graph actor
E = "https://x.example/users/e"  # bare graph node (referenced, never fetched)


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


class StubHandler:
    def next_available(self, job):
        return 0

    async def handle(self, job):
        pass


def dispatcher(redis):
    dis = Dispatcher(redis)
    for job_type in ("actor", "webfinger", "page", "collection"):
        dis.set_handler(job_type, StubHandler())
    return dis


def actor_job(actor_id, depth=1):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def webfinger_job(handle):
    return {"job_type": "webfinger", "webfinger": handle}


async def test_seeds_seen_from_queue_inflight_and_failed():
    r = fake_redis()
    dis = dispatcher(r)
    await dis.enqueue(actor_job(A))
    await dis.enqueue(actor_job(B))
    await dis.get()  # leases one of them -> in flight; the other stays queued
    await dis.fail(webfinger_job(C))

    await fixup_seen(r, FakeGraph())

    assert await dis.seen(actor_job(A))  # one queued, one in flight -- both seeded
    assert await dis.seen(actor_job(B))
    assert await dis.seen(webfinger_job(C))  # failed -> seeded


async def test_seeds_fetched_actors_but_not_bare_nodes():
    r = fake_redis()
    dis = dispatcher(r)
    graph = FakeGraph()
    await graph.ensure_node(D)
    await graph.set_node_property(D, "last_fetch_date", "2024-06-15T00:00:00Z")
    await graph.ensure_node(E)  # bare: known but never fetched

    await fixup_seen(r, graph)

    assert await dis.seen(actor_job(D))  # fetched -> seen, won't be re-crawled
    assert not await dis.seen(actor_job(E))  # bare -> still crawlable


async def test_dedupes_duplicate_queue_members():
    r = fake_redis()
    dis = dispatcher(r)
    await dis.enqueue(actor_job(A, depth=1))
    await dis.enqueue(actor_job(A, depth=2))  # same job_id, distinct member
    assert await r.zcard(QUEUE) == 2

    await fixup_seen(r, FakeGraph())

    assert await r.zcard(QUEUE) == 1  # collapsed to a single copy
    assert await dis.seen(actor_job(A))


async def test_drops_queued_jobs_already_in_flight():
    r = fake_redis()
    dis = dispatcher(r)
    await dis.enqueue(actor_job(B, depth=1))
    await dis.get()  # B -> in flight, off the queue
    await dis.enqueue(actor_job(B, depth=2))  # a stale duplicate left on the queue
    assert await r.zcard(QUEUE) == 1

    await fixup_seen(r, FakeGraph())

    # A queued copy of an already-in-flight job is redundant -> removed.
    assert await r.zcard(QUEUE) == 0
    assert await dis.seen(actor_job(B))


async def test_keeps_malformed_queue_members_but_never_seeds_them():
    r = fake_redis()
    # The Dispatcher refuses to enqueue an unidentifiable job, so inject the raw
    # member directly: an actor job with no actor_id (job_id -> None).
    bad = json.dumps({"job_type": "actor", "depth": 1}, sort_keys=True)
    await r.zadd(QUEUE, {f"{iso_utc(0)}|{bad}": 0})
    assert await r.zcard(QUEUE) == 1

    await fixup_seen(r, FakeGraph())

    # Minimize destructiveness: a member we can't identify is LEFT on the queue
    # (it may carry useful info) -- but it is never seeded into SEEN.
    assert await r.zcard(QUEUE) == 1
    assert await r.scard(SEEN) == 0


async def test_is_idempotent_across_two_runs():
    r = fake_redis()
    dis = dispatcher(r)
    await dis.enqueue(actor_job(A, depth=1))
    await dis.enqueue(actor_job(A, depth=2))  # duplicate
    await dis.enqueue(actor_job(B))

    await fixup_seen(r, FakeGraph())
    queue_after_first = await r.zcard(QUEUE)
    seen_after_first = await r.scard(SEEN)

    await fixup_seen(r, FakeGraph())

    # A second run must not shrink the queue further or alter SEEN.
    assert await r.zcard(QUEUE) == queue_after_first == 2
    assert await r.scard(SEEN) == seen_after_first == 2
    assert await dis.seen(actor_job(A))
    assert await dis.seen(actor_job(B))


async def test_dedupes_across_batch_boundaries():
    # batch_size=1 makes every member its own flush, so each duplicate is split
    # across separate batches -- exercising the cross-batch path (SEEN persists
    # between flushes) and the count partitioning, which the default-batch tests
    # (everything in one flush) can't reach.
    third = "https://x.example/users/cc"
    r = fake_redis()
    dis = dispatcher(r)
    await dis.enqueue(actor_job(A, depth=1))
    await dis.enqueue(actor_job(A, depth=2))  # dup of A
    await dis.enqueue(actor_job(B, depth=1))
    await dis.enqueue(actor_job(B, depth=2))  # dup of B
    await dis.enqueue(actor_job(third))
    await r.delete(SEEN)  # clear what enqueue() seeded, as fixup_seen's del_seen does

    tried, succeeded, deduped = await add_queue_to_seen_and_dedupe(r, batch_size=1)

    # 5 queued members, 3 distinct ids: 3 kept (added), 2 duplicates removed.
    assert (tried, succeeded, deduped) == (5, 3, 2)
    assert await r.zcard(QUEUE) == 3  # one copy per distinct id survives
    assert await dis.seen(actor_job(A))
    assert await dis.seen(actor_job(B))
    assert await dis.seen(actor_job(third))
