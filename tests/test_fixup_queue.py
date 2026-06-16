"""Tests for fixup_queue — migrate queue members from `counter:job` to `ts|job`.

The old dispatcher wrote members as `f"{counter}:{job_json}"`; the new one writes
`f"{iso_utc(ms)}|{job_json}"`. fixup_queue rewrites the former into the latter in
place, synthesizing the timestamp from the counter (epoch + counter ms) so the
original FIFO order is preserved, and carrying each member's score over unchanged.

A FakeAsyncRedis stands in for Valkey. Assumptions to flag if the shape differs:
  - members are `prefix<sep>job_json`; old sep is ':', new sep is '|'.
  - the score on each ZSET member is next_available and must survive untouched.
  - the migration is idempotent: a second run is a no-op.
"""

import json

import pytest
from fakeredis import FakeAsyncRedis, FakeServer

from fixup_queue import WRITE_BATCH, fixup_queue
from pub_crawler.dispatcher import QUEUE, iso_utc


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def job_str(job):
    # Same canonical form the dispatcher uses for the job half of a member.
    return json.dumps(job, sort_keys=True)


async def members_with_scores(r):
    """The raw ZSET members (decoded) with their scores, in score/lex order."""
    return [(m.decode(), s) for m, s in await r.zrange(QUEUE, 0, -1, withscores=True)]


async def test_rewrites_old_members_to_ts_pipe_format():
    r = fake_redis()
    job = {"job_type": "webfinger", "webfinger": "evan@cosocial.example"}
    await r.zadd(QUEUE, {f"7:{job_str(job)}": 1000.0})

    migrated = await fixup_queue(r)

    assert migrated == 1
    [(member, score)] = await members_with_scores(r)
    # Timestamp synthesized from the counter (7 ms past epoch), job half intact.
    assert member == f"{iso_utc(7)}|{job_str(job)}"
    # The job round-trips, and the priority (score) is carried over unchanged.
    assert json.loads(member.split("|", 1)[1]) == job
    assert score == 1000.0


async def test_preserves_fifo_order_of_the_old_counters():
    r = fake_redis()
    # Same score => FIFO is decided by the member tiebreak. Counters cross a
    # digit-width boundary (2 vs 10), exactly where the old lexicographic order
    # diverged from insertion order.
    for counter in (2, 10, 91, 411192):
        job = {"job_type": "actor", "n": counter}
        await r.zadd(QUEUE, {f"{counter}:{job_str(job)}": 500.0})

    await fixup_queue(r)

    order = [
        json.loads(m.split("|", 1)[1])["n"] for m, _ in await members_with_scores(r)
    ]
    assert order == [2, 10, 91, 411192]  # numeric insertion order, not lexicographic


async def test_is_idempotent_and_leaves_new_members_alone():
    r = fake_redis()
    job = {"job_type": "webfinger", "webfinger": "alice@social.example"}
    await r.zadd(QUEUE, {f"3:{job_str(job)}": 200.0})

    first = await fixup_queue(r)
    after_first = await members_with_scores(r)
    second = await fixup_queue(r)
    after_second = await members_with_scores(r)

    assert first == 1
    assert second == 0  # nothing left in the old format
    assert after_first == after_second  # already-migrated members untouched


async def test_migrates_more_members_than_one_write_batch():
    # Cross several write-batch boundaries so the flush/clear logic is exercised,
    # not just a single trailing flush. Every member must be migrated exactly once.
    r = fake_redis()
    count = WRITE_BATCH * 2 + 5
    for counter in range(count):
        job = {"job_type": "actor", "n": counter}
        await r.zadd(QUEUE, {f"{counter}:{job_str(job)}": 500.0})

    migrated = await fixup_queue(r)

    assert migrated == count
    members = await members_with_scores(r)
    assert len(members) == count
    # Every surviving member is in the new format (none left as counter:job)...
    assert all("|" in m for m, _ in members)
    # ...and the full set of jobs round-trips intact, none dropped or duplicated.
    ns = sorted(json.loads(m.split("|", 1)[1])["n"] for m, _ in members)
    assert ns == list(range(count))
