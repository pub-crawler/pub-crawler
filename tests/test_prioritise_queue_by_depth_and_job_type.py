"""Tests for prioritise_queue_by_depth_and_job_type -- the one-shot migration
that rewrites existing queue members from the old `ts|job` format into the new
`depth|type|ts|job` format the dispatcher now sorts on.

Contract:
  - For every OLD-format member (`ts|job`), rewrite it in place to
    `depth|type|ts|job`: depth (zero-padded, width 2) from job["depth"]
    (default 0), the job-type code (webfinger=10, actor=20, collection=30,
    page=40, also width 2), the ORIGINAL ts preserved (so FIFO order survives),
    and the job unchanged. The score (next_available) is preserved.
  - NEW-format members (already `depth|type|ts|job`) are left untouched.
  - Idempotent -- a second run is a no-op.
  - Run with the crawler STOPPED (it rewrites queue members).

Black-box: written from the contract above; uses fakeredis and inspects the
queue ZSET directly. Scores are whole numbers so the tests don't care whether
the migration floors or preserves the score exactly.

Assumptions to flag if they differ: the function name, the four type codes, the
width-2 zero padding, and missing-depth -> 0.
"""

import json

from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import QUEUE
from prioritise_queue_by_depth_and_job_type import prioritise_queue_by_depth_and_job_type

TS = "2026-06-17T12:00:00.000000Z"


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def _job_str(job):
    return json.dumps(job, sort_keys=True)


def old_member(job, ts=TS):
    """An old-format `ts|job` queue member."""
    return f"{ts}|{_job_str(job)}"


def new_member(job, depth_code, type_code, ts=TS):
    """A new-format `depth|type|ts|job` queue member."""
    return f"{depth_code}|{type_code}|{ts}|{_job_str(job)}"


def actor(actor_id="https://x.example/users/a", depth=1):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def webfinger(handle="acct:a@x.example"):
    return {"job_type": "webfinger", "webfinger": handle}


async def queue(r):
    """{member_str: score} for the whole queue."""
    return {m.decode(): s for m, s in await r.zrange(QUEUE, 0, -1, withscores=True)}


# ---------------------------------------------------------------------------
# Rewrite: old -> new, preserving ts and score
# ---------------------------------------------------------------------------


async def test_rewrites_old_member_to_new_format():
    r = fake_redis()
    job = actor(depth=2)
    await r.zadd(QUEUE, {old_member(job): 1500})

    await prioritise_queue_by_depth_and_job_type(r)

    members = await queue(r)
    assert len(members) == 1
    member, score = next(iter(members.items()))
    depth_code, type_code, ts, job_json = member.split("|", 3)
    assert depth_code == "02"  # depth 2, zero-padded width 2
    assert type_code == "20"  # actor
    assert ts == TS  # original ts preserved (FIFO order survives)
    assert json.loads(job_json) == job  # job unchanged
    assert score == 1500  # score preserved


async def test_missing_depth_defaults_to_zero():
    r = fake_redis()
    await r.zadd(QUEUE, {old_member(webfinger()): 5})  # webfinger carries no depth

    await prioritise_queue_by_depth_and_job_type(r)

    [member] = (await queue(r)).keys()
    depth_code, type_code, _, _ = member.split("|", 3)
    assert depth_code == "00"  # missing depth -> 0
    assert type_code == "10"  # webfinger


async def test_encodes_each_job_type_code():
    r = fake_redis()
    jobs = [
        webfinger(),
        actor(actor_id="https://x.example/users/a", depth=0),
        {"job_type": "collection", "collection_id": "https://x.example/c", "depth": 0},
        {"job_type": "page", "page_id": "https://x.example/p", "depth": 0},
    ]
    for i, job in enumerate(jobs):
        await r.zadd(QUEUE, {old_member(job): i})

    await prioritise_queue_by_depth_and_job_type(r)

    codes = {}
    for member in (await queue(r)).keys():
        _, type_code, _, job_json = member.split("|", 3)
        codes[json.loads(job_json)["job_type"]] = type_code
    assert codes == {"webfinger": "10", "actor": "20", "collection": "30", "page": "40"}


# ---------------------------------------------------------------------------
# Leave new-format members alone; idempotent
# ---------------------------------------------------------------------------


async def test_leaves_new_format_members_unchanged():
    r = fake_redis()
    already = new_member(actor(depth=3), "03", "20")
    await r.zadd(QUEUE, {already: 700})

    await prioritise_queue_by_depth_and_job_type(r)

    assert await queue(r) == {already: 700}  # byte-for-byte untouched


async def test_is_idempotent():
    r = fake_redis()
    await r.zadd(QUEUE, {old_member(actor(depth=1)): 100})

    await prioritise_queue_by_depth_and_job_type(r)
    after_first = await queue(r)
    await prioritise_queue_by_depth_and_job_type(r)
    after_second = await queue(r)

    assert after_first == after_second  # second run is a no-op


async def test_mixed_queue_rewrites_only_old_and_preserves_count():
    r = fake_redis()
    old = old_member(actor(actor_id="https://x.example/users/old", depth=1))
    already = new_member(
        {"job_type": "page", "page_id": "https://x.example/p", "depth": 3}, "03", "40"
    )
    await r.zadd(QUEUE, {old: 1, already: 2})

    await prioritise_queue_by_depth_and_job_type(r)

    members = await queue(r)
    assert len(members) == 2  # count preserved
    assert already in members  # the new one is untouched
    assert old not in members  # the old one was replaced
    rewritten = next(m for m in members if m != already)
    assert len(rewritten.split("|", 3)) == 4  # now in 4-field new format
