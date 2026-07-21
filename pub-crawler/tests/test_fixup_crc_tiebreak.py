"""Tests for fixup_crc_tiebreak -- migrate existing queue members from the old
`depth|type|ts|job` (4-field) format to the `depth|type|crc|ts|job` (5-field)
format the crc32-tiebreak dispatcher reads. Without it, the new
Dispatcher._member_to_job (which unpacks 5 fields) blows up on the ~800k
members already on the live queue.

Contract:
  - Each OLD 4-field member gets a crc field spliced in between `type` and
    `ts`; the original ts and the job JSON are preserved verbatim.
  - The spliced crc EQUALS the crc the dispatcher itself writes for that job,
    so migrated and freshly-enqueued members interleave consistently. The
    tests pin this by COMPARISON to a real Dispatcher, not by hard-coding the
    hash -- so the hash function stays swappable.
  - NEW 5-field members are left untouched -> idempotent, safe to re-run.
  - An empty queue is a no-op. Scores are preserved.
  - Returns the number of members processed.
  - Run with the crawler STOPPED (it rewrites queue members while iterating).

Black-box over fakeredis; a real Dispatcher supplies the canonical crc.
"""

import json

from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import Dispatcher, QUEUE
from fixup_crc_tiebreak import fixup_crc_tiebreak

TS = "2026-07-21T12:00:00.000000Z"


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def page(tag, depth=2):
    return {
        "job_type": "page",
        "page_id": f"https://x.example/users/{tag}/followers?page=1",
        "depth": depth,
    }


def old_member(depth_code, type_code, job, ts=TS):
    """A pre-migration 4-field `depth|type|ts|job` queue member."""
    return f"{depth_code}|{type_code}|{ts}|{json.dumps(job, sort_keys=True)}"


async def queue(r):
    """{member_str: score} for the whole queue."""
    return {m.decode(): s for m, s in await r.zrange(QUEUE, 0, -1, withscores=True)}


async def dispatcher_member(job):
    """The full 5-field member a real Dispatcher writes for `job`."""
    d = Dispatcher(fake_redis())
    await d.enqueue(job)  # enqueue no longer needs a handler
    [m] = await d.redis.zrange(QUEUE, 0, -1)
    return m.decode()


def crc_field(member):
    # depth|type|CRC|ts|job -- the crc is the 3rd field.
    return member.split("|", 4)[2]


# ---------------------------------------------------------------------------
# migrate: splice the crc, preserve ts + job
# ---------------------------------------------------------------------------


async def test_migrates_to_five_fields_preserving_ts_and_job():
    r = fake_redis()
    job = page("bob", depth=2)
    await r.zadd(QUEUE, {old_member("02", "40", job, ts=TS): 0})

    await fixup_crc_tiebreak(r)

    [migrated] = list(await queue(r))
    parts = migrated.split("|", 4)
    assert len(parts) == 5
    assert parts[0] == "02"  # depth prefix unchanged
    assert parts[1] == "40"  # type prefix unchanged
    assert parts[3] == TS  # original ts preserved
    assert json.loads(parts[4]) == job  # job preserved


async def test_spliced_crc_matches_the_dispatcher():
    # The whole point: a migrated member and a dispatcher-enqueued member for
    # the SAME job carry the SAME crc, so they sort together consistently.
    r = fake_redis()
    job = page("alice", depth=1)
    await r.zadd(QUEUE, {old_member("01", "40", job): 0})

    await fixup_crc_tiebreak(r)

    [migrated] = list(await queue(r))
    assert crc_field(migrated) == crc_field(await dispatcher_member(job))


# ---------------------------------------------------------------------------
# idempotence + edges
# ---------------------------------------------------------------------------


async def test_already_migrated_member_is_untouched():
    # A real dispatcher-produced 5-field member must survive a fixup run byte
    # for byte -- the migration must not double-splice.
    r = fake_redis()
    m5 = await dispatcher_member(page("carol", depth=1))
    await r.zadd(QUEUE, {m5: 0})

    await fixup_crc_tiebreak(r)

    assert list(await queue(r)) == [m5]


async def test_second_run_is_a_noop():
    r = fake_redis()
    await r.zadd(QUEUE, {old_member("01", "40", page("dave", depth=1)): 0})

    await fixup_crc_tiebreak(r)
    once = await queue(r)
    await fixup_crc_tiebreak(r)

    assert await queue(r) == once


async def test_empty_queue_is_a_noop():
    r = fake_redis()

    count = await fixup_crc_tiebreak(r)

    assert count == 0
    assert await queue(r) == {}


async def test_score_is_preserved():
    r = fake_redis()
    await r.zadd(QUEUE, {old_member("01", "40", page("erin", depth=1)): 0})

    await fixup_crc_tiebreak(r)

    assert all(score == 0 for score in (await queue(r)).values())


async def test_batching_migrates_every_member():
    # More members than one write batch: end state is all-migrated. The scan
    # may re-see rewritten (already-5-field) members, so the processed count is
    # a lower bound.
    r = fake_redis()
    jobs = [page(f"u{i}", depth=2) for i in range(5)]
    await r.zadd(QUEUE, {old_member("02", "40", j): 0 for j in jobs})

    count = await fixup_crc_tiebreak(r, batch_size=2)

    assert count >= 5
    migrated = list(await queue(r))
    assert len(migrated) == 5
    assert all(len(m.split("|", 4)) == 5 for m in migrated)  # every one migrated
