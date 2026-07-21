"""Tests for recover_failed — re-enqueue recoverable jobs from the failed set.

bin/recover_failed.py exports `async def recover_failed(dispatcher, G)`. It
walks the dispatcher.failed() iterator and decides per job:

  - page and collection jobs: always recoverable — re-enqueue.
  - actor jobs: recoverable only if the graph holds at least one actor on the
    same hostname that was SUCCESSFULLY fetched (the fetch-success properties
    ActorHandler writes: last_fetch_date / hostname / http_status 200). A bare
    node (discovered on a page but never fetched) or a node whose fetch came
    back an HTTP error does not make the host recoverable.
  - webfinger jobs: never recovered; they stay in the failed set.

Every job it re-enqueues it also removes from the failed set (via
dispatcher.unfail), so running recovery twice doesn't double-enqueue; jobs it
skips stay in the failed set for a later pass.

A failed job was necessarily enqueued once already, so its id is in the
dispatcher's SEEN set. Recovery must re-enqueue anyway — an implementation
that gates on `dispatcher.seen(job)` recovers nothing in production. The
crash() helper runs every job through the real enqueue -> get -> fail
lifecycle precisely so that the seen-mark is in place and a seen-gated
implementation fails these tests.

Uses the real Dispatcher over FakeAsyncRedis (as test_add_seeds does) and the
FakeGraph stand-in from support.
"""

import json

from fakeredis import FakeAsyncRedis, FakeServer

from recover_failed import recover_failed
from pub_crawler.dispatcher import Dispatcher, QUEUE
from support import FakeGraph


def fake_redis():
    # Fresh, isolated in-memory async Redis (its own server) per call.
    return FakeAsyncRedis(server=FakeServer())


class ReadyHandler:
    """next_available in the past, so enqueue() can score jobs and get() hands
    them straight back. Recovery itself never dispatches; this is only the
    plumbing enqueue/get need."""

    def next_available(self, job):
        return 0


def dispatcher():
    d = Dispatcher(fake_redis())
    for job_type in ("webfinger", "actor", "collection", "page"):
        d.set_handler(job_type, ReadyHandler())
    return d


async def crash(d, job):
    """Run a job through its real lifecycle into the failed set: enqueue (which
    marks it seen), lease it as a worker would, and fail it. Leaves the queue
    empty and the job's id in SEEN — exactly the state recovery starts from."""
    await d.enqueue(job)
    assert await d.get() == job
    await d.fail(job)


async def queued_jobs(d):
    """The jobs currently on the queue ZSET, parsed back out of the
    `depth|type|crc|ts|job` member format (see Dispatcher._job_to_member)."""
    members = await d.redis.zrange(QUEUE, 0, -1)
    return [json.loads(member.decode().split("|", 4)[4]) for member in members]


async def collect_failed(d):
    return [job async for job in d.failed()]


# --- graph states, as the crawler actually leaves them ----------------------


async def fetched_actor(g, actor_id, hostname):
    """A node exactly as ActorHandler leaves a successful (HTTP 200) fetch."""
    await g.ensure_node(actor_id)
    await g.set_node_properties(
        actor_id,
        {
            "http_status": 200,
            "last_fetch_date": "2026-07-01T00:00:00+00:00",
            "depth": 1,
            "hostname": hostname,
        },
    )


async def discovered_actor(g, actor_id):
    """A bare node, as page ingestion creates it: known about, never fetched,
    no properties at all."""
    await g.ensure_node(actor_id)


async def gone_actor(g, actor_id):
    """A node whose fetch failed: ActorHandler records only the error status
    (no last_fetch_date, no hostname)."""
    await g.ensure_node(actor_id)
    await g.set_node_properties(actor_id, {"http_status": 410})


# --- job shapes matching what's actually in the failed set ------------------

HOST = "mastodon.example"


def page_job(host=HOST, page=44):
    return {
        "job_type": "page",
        "direction": "followers",
        "owner_id": f"https://{host}/users/owner",
        "page_id": f"https://{host}/users/owner/followers?page={page}",
        "depth": 0,
    }


def collection_job(host=HOST):
    return {
        "job_type": "collection",
        "collection_id": f"https://{host}/users/owner/following",
        "owner_id": f"https://{host}/users/owner",
        "direction": "following",
        "depth": 0,
    }


def actor_job(name="bob", host=HOST):
    return {
        "job_type": "actor",
        "actor_id": f"https://{host}/users/{name}",
        "depth": 0,
    }


def webfinger_job():
    # Real failed webfinger jobs are seed-time and carry no depth key.
    return {"job_type": "webfinger", "webfinger": "evan@cosocial.example"}


# ---------------------------------------------------------------------------
# page and collection jobs: always recovered
# ---------------------------------------------------------------------------


async def test_reenqueues_a_failed_page_job():
    d = dispatcher()
    job = page_job()
    await crash(d, job)

    await recover_failed(d, FakeGraph())

    # Back on the queue with every field intact (depth included)...
    assert await queued_jobs(d) == [job]
    # ...and off the failed set, so a re-run won't double-enqueue it.
    assert await collect_failed(d) == []


async def test_reenqueues_a_failed_collection_job():
    d = dispatcher()
    job = collection_job()
    await crash(d, job)

    await recover_failed(d, FakeGraph())

    assert await queued_jobs(d) == [job]
    assert await collect_failed(d) == []


async def test_reenqueues_even_though_failed_jobs_are_already_seen():
    # The de-dup gate handlers use (`if not seen: enqueue`) must NOT apply
    # here: every failed job is already seen from its first enqueue.
    d = dispatcher()
    job = page_job()
    await crash(d, job)
    assert await d.seen(job)

    await recover_failed(d, FakeGraph())

    assert await queued_jobs(d) == [job]


# ---------------------------------------------------------------------------
# actor jobs: recovered only if the host has a successfully fetched actor
# ---------------------------------------------------------------------------


async def test_recovers_an_actor_job_when_a_same_host_actor_was_fetched():
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, f"https://{HOST}/users/alice", HOST)
    job = actor_job(name="bob", host=HOST)
    await crash(d, job)

    await recover_failed(d, g)

    assert await queued_jobs(d) == [job]
    assert await collect_failed(d) == []


async def test_skips_an_actor_job_when_no_same_host_actor_was_fetched():
    d = dispatcher()
    g = FakeGraph()
    # A healthy actor elsewhere doesn't vouch for this job's host.
    await fetched_actor(g, "https://other.example/users/alice", "other.example")
    job = actor_job(host="dead.example")
    await crash(d, job)

    await recover_failed(d, g)

    # Not re-enqueued, and left in the failed set for a later pass.
    assert await queued_jobs(d) == []
    assert await collect_failed(d) == [job]


async def test_a_discovered_but_unfetched_neighbour_does_not_recover_an_actor():
    # Bare nodes exist for every actor a page ever mentioned; only a completed
    # fetch proves the host answers.
    d = dispatcher()
    g = FakeGraph()
    await discovered_actor(g, f"https://{HOST}/users/alice")
    job = actor_job(name="bob", host=HOST)
    await crash(d, job)

    await recover_failed(d, g)

    assert await queued_jobs(d) == []
    assert await collect_failed(d) == [job]


async def test_a_failed_fetch_neighbour_does_not_recover_an_actor():
    # An HTTP-error fetch (e.g. 410) is evidence against the host, not for it.
    d = dispatcher()
    g = FakeGraph()
    await gone_actor(g, f"https://{HOST}/users/alice")
    job = actor_job(name="bob", host=HOST)
    await crash(d, job)

    await recover_failed(d, g)

    assert await queued_jobs(d) == []
    assert await collect_failed(d) == [job]


# ---------------------------------------------------------------------------
# webfinger jobs: never recovered
# ---------------------------------------------------------------------------


async def test_skips_webfinger_jobs():
    d = dispatcher()
    job = webfinger_job()
    await crash(d, job)

    await recover_failed(d, FakeGraph())

    assert await queued_jobs(d) == []
    assert await collect_failed(d) == [job]


# ---------------------------------------------------------------------------
# whole-set behaviour
# ---------------------------------------------------------------------------


async def test_an_empty_failed_set_is_a_noop():
    d = dispatcher()

    await recover_failed(d, FakeGraph())

    assert await queued_jobs(d) == []


async def test_mixed_failed_set_recovers_only_the_recoverable():
    d = dispatcher()
    g = FakeGraph()
    await fetched_actor(g, f"https://{HOST}/users/alice", HOST)

    recoverable = [
        page_job(),
        collection_job(),
        actor_job(name="bob", host=HOST),
    ]
    stuck = [
        actor_job(name="carol", host="dead.example"),
        webfinger_job(),
    ]
    for job in recoverable + stuck:
        await crash(d, job)

    await recover_failed(d, g)

    # Queue order isn't promised (equal scores); compare membership.
    queued = await queued_jobs(d)
    assert len(queued) == len(recoverable)
    for job in recoverable:
        assert job in queued

    remaining = await collect_failed(d)
    assert len(remaining) == len(stuck)
    for job in stuck:
        assert job in remaining


async def test_a_second_run_is_a_noop():
    # Everything recoverable was unfailed on the first pass, so a re-run finds
    # nothing to enqueue and the queue doesn't grow.
    d = dispatcher()
    job = page_job()
    await crash(d, job)

    await recover_failed(d, FakeGraph())
    await recover_failed(d, FakeGraph())

    assert await queued_jobs(d) == [job]
