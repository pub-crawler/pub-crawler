"""Tests for add_seeds — enqueue webfinger seed jobs onto the Redis queue.

add_seeds reads webfinger addresses (one per line, blanks skipped) from a file
and enqueues a {job_type:'webfinger', webfinger:wf} job for each onto the
dispatcher's Redis ZSET. It does NOT process them (no graph, no fetch) — it only
seeds the queue for a crawler to drain later, so seeding must make no HTTP calls.

A FakeAsyncRedis stands in for Valkey; the queue is read back and the members
parsed the same way Dispatcher.enqueue writes them ("{counter}:{job_json}").
"""

import json

import httpx
from fakeredis import FakeAsyncRedis, FakeServer

from add_seeds import add_seeds
from pub_crawler.dispatcher import QUEUE


def fake_redis():
    # Fresh, isolated in-memory async Redis (its own server) per call.
    return FakeAsyncRedis(server=FakeServer())


def no_http(request):
    # Seeding only *scores* jobs (a non-consuming peek at the throttle counter);
    # it must never actually fetch. Any request here is a bug.
    raise AssertionError(f"unexpected HTTP during seeding: {request.url}")


async def queued_jobs(r):
    """The jobs currently on the queue ZSET, in score order, parsed back."""
    members = await r.zrange(QUEUE, 0, -1)
    jobs = []
    for member in members:
        _ts, job_json = member.decode().split("|", 1)
        jobs.append(json.loads(job_json))
    return jobs


async def test_enqueues_a_webfinger_job_per_seed(tmp_path):
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("evan@cosocial.ca\nalice@example.social\n")
    r = fake_redis()

    await add_seeds(str(seeds), r, transport=httpx.MockTransport(no_http))

    # FIFO by enqueue order (the counter tiebreaks the equal ~now scores).
    assert await queued_jobs(r) == [
        {"job_type": "webfinger", "webfinger": "evan@cosocial.ca"},
        {"job_type": "webfinger", "webfinger": "alice@example.social"},
    ]


async def test_skips_blank_and_whitespace_lines(tmp_path):
    seeds = tmp_path / "seeds.txt"
    seeds.write_text("  evan@cosocial.ca  \n\n\n   \nalice@example.social\n")
    r = fake_redis()

    await add_seeds(str(seeds), r, transport=httpx.MockTransport(no_http))

    webfingers = [j["webfinger"] for j in await queued_jobs(r)]
    assert webfingers == ["evan@cosocial.ca", "alice@example.social"]


async def test_follow_up_seeds_append_to_an_existing_queue(tmp_path):
    # The "initial or follow-up" use: a second run adds to what's already queued.
    first = tmp_path / "first.txt"
    first.write_text("evan@cosocial.ca\n")
    second = tmp_path / "second.txt"
    second.write_text("bob@example.social\n")
    r = fake_redis()

    await add_seeds(str(first), r, transport=httpx.MockTransport(no_http))
    await add_seeds(str(second), r, transport=httpx.MockTransport(no_http))

    webfingers = [j["webfinger"] for j in await queued_jobs(r)]
    assert set(webfingers) == {"evan@cosocial.ca", "bob@example.social"}
