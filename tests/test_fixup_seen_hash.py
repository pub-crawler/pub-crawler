"""Tests for fixup_seen_hash -- the one-shot migration that rewrites the SEEN
oracle from full job-id strings (key SEEN) into fixed-width hashes (key
SEEN_HASHED) to reclaim Redis memory.

Memory-safe by construction: it SSCANs SEEN in batches and, per batch, adds the
hashes to SEEN_HASHED and SREMs the strings from SEEN -- so the old set shrinks
as the new grows (and hashes are smaller than strings, so total memory declines
throughout). The old key is DELeted once empty.

Contract:
  - Every id in the old SEEN becomes recognizable via SEEN_HASHED: after the
    run a Dispatcher's seen(job) is True for every job whose id was in SEEN.
    This is the correctness link -- the fixup must hash IDENTICALLY to the
    dispatcher, which runs in a different process.
  - The old SEEN key is emptied and deleted.
  - SEEN_HASHED holds one hash per unique old id.
  - Batching covers every member (mid-scan flush + final partial batch).
  - Resumable: an interrupted run (some ids already moved to SEEN_HASHED and
    gone from SEEN) completes correctly on re-run; a second full run is a no-op.
  - Empty/absent old SEEN is a no-op (returns 0).

Behavioral: membership is checked through Dispatcher.seen() rather than by
recomputing the hash, so these stay agnostic to the hash function (the
dispatcher suite pins that, and the shared hash is what ties the two together).

Assumptions to flag if they differ: the function name, the (r, batch_size=...)
signature, and that it reads dispatcher.SEEN and writes dispatcher.SEEN_HASHED.
"""

from fakeredis import FakeAsyncRedis, FakeServer

from pub_crawler.dispatcher import Dispatcher, SEEN, SEEN_HASHED
from pub_crawler.job_id import job_id
from fixup_seen_hash import fixup_seen_hash


def fake_redis():
    return FakeAsyncRedis(server=FakeServer())


def actor_job(u):
    return {"job_type": "actor", "actor_id": f"https://x.example/users/{u}", "depth": 1}


async def seed_legacy(r, jobs):
    """Populate the OLD SEEN with job-id strings, as the pre-hash crawler did."""
    for j in jobs:
        await r.sadd(SEEN, job_id(j))


# ---------------------------------------------------------------------------
# correctness: migrated ids are recognized by the (separate-process) dispatcher
# ---------------------------------------------------------------------------


async def test_migrated_ids_are_seen_by_the_dispatcher():
    r = fake_redis()
    jobs = [actor_job(u) for u in ("a", "b", "c")]
    await seed_legacy(r, jobs)

    await fixup_seen_hash(r)

    dis = Dispatcher(r)
    for j in jobs:
        assert await dis.seen(j)


async def test_old_set_is_emptied_and_deleted():
    r = fake_redis()
    await seed_legacy(r, [actor_job("a"), actor_job("b")])

    await fixup_seen_hash(r)

    assert await r.exists(SEEN) == 0


async def test_one_hash_per_unique_id():
    r = fake_redis()
    await seed_legacy(r, [actor_job(u) for u in ("a", "b", "c", "d")])

    await fixup_seen_hash(r)

    assert await r.scard(SEEN_HASHED) == 4


# ---------------------------------------------------------------------------
# batching, resumability, idempotence, edges
# ---------------------------------------------------------------------------


async def test_batching_migrates_every_member():
    # More members than one batch: both the mid-scan flush and the final
    # partial-batch flush must run.
    r = fake_redis()
    jobs = [actor_job(str(i)) for i in range(7)]
    await seed_legacy(r, jobs)

    await fixup_seen_hash(r, batch_size=3)

    dis = Dispatcher(r)
    assert all([await dis.seen(j) for j in jobs])
    assert await r.exists(SEEN) == 0


async def test_resumes_after_a_partial_run():
    # Simulate an interrupted fixup: one id already migrated (hash in
    # SEEN_HASHED, string already gone from SEEN), the rest still strings.
    r = fake_redis()
    done, rest = actor_job("done"), [actor_job("x"), actor_job("y")]
    await Dispatcher(r).enqueue(done)  # done's hash is already in SEEN_HASHED
    await seed_legacy(r, rest)  # x, y are still strings in SEEN

    await fixup_seen_hash(r)

    dis = Dispatcher(r)
    for j in [done, *rest]:
        assert await dis.seen(j)
    assert await r.exists(SEEN) == 0
    assert await r.scard(SEEN_HASHED) == 3


async def test_second_run_is_a_noop():
    r = fake_redis()
    jobs = [actor_job("a"), actor_job("b")]
    await seed_legacy(r, jobs)

    await fixup_seen_hash(r)
    after_first = await r.scard(SEEN_HASHED)
    await fixup_seen_hash(r)

    assert await r.scard(SEEN_HASHED) == after_first == 2
    dis = Dispatcher(r)
    assert all([await dis.seen(j) for j in jobs])


async def test_empty_old_set_is_a_noop():
    r = fake_redis()

    count = await fixup_seen_hash(r)

    assert count == 0
    assert await r.scard(SEEN_HASHED) == 0


async def test_returns_count_migrated():
    r = fake_redis()
    jobs = [actor_job(str(i)) for i in range(5)]
    await seed_legacy(r, jobs)

    count = await fixup_seen_hash(r, batch_size=2)

    # SSCAN may re-deliver members, so the processed count is a lower bound;
    # the end-state is the real assertion.
    assert count >= 5
    dis = Dispatcher(r)
    assert all([await dis.seen(j) for j in jobs])
