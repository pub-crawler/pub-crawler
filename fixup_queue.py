"""One-off migration: rewrite queue members from the old `counter:job` format
to the new `ts|job` format used by the dispatcher.

Old members were `f"{counter}:{job_json}"`, where the counter was an unpadded
integer that broke FIFO under Redis' lexicographic tiebreak. New members are
`f"{iso_utc(ms)}|{job_json}"`. To preserve the original FIFO order we synthesize
the timestamp as epoch + counter milliseconds, i.e. iso_utc(counter): the
counter is monotonic, so the resulting timestamps stay monotonic and sort
correctly. Scores (next_available) are carried over unchanged.

Idempotent: members already in the new format are left alone. Detection keys on
the prefix before the first ':' — for an old member that prefix is all digits
(the counter); for a new member it is the start of the ISO timestamp
("2024-06-15T12"), which is not.

Run with the crawler/dispatcher STOPPED: a concurrent bzpopmin could pop a member
mid-rewrite and lose or duplicate a job.

The queue can hold hundreds of thousands of members, so we iterate with ZSCAN
(a cursor) rather than reading the whole set at once -- a single
`ZRANGE 0 -1 WITHSCORES` builds one enormous reply that overruns the client's
socket read timeout on a managed Redis/Valkey. Rewrites go out in batched
pipelines to keep the round-trip count (and wall-clock) down, and only one batch
is held in memory at a time so a tight pod memory limit is fine.
"""

import logging

from pub_crawler.dispatcher import QUEUE, iso_utc

SCAN_COUNT = 1000   # ZSCAN per-call batch hint (fewer round-trips than the default)
WRITE_BATCH = 500   # members rewritten per pipeline


def _is_old_member(member):
    head, sep, _ = member.partition(":")
    return bool(sep) and head.isdigit()


async def fixup_queue(r):
    """Rewrite every old-format member of the QUEUE ZSET in place.

    Returns the number of members migrated. Idempotent: re-running skips members
    already in `ts|job` form, so it is safe to run again if interrupted.
    """
    migrated = 0
    batch = []  # (old_member_bytes, new_member_str, score) awaiting a flush

    async def flush():
        nonlocal migrated
        if not batch:
            return
        async with r.pipeline(transaction=True) as pipe:
            for old_b, new_member, score in batch:
                pipe.zrem(QUEUE, old_b)
                pipe.zadd(QUEUE, {new_member: score})
            await pipe.execute()
        migrated += len(batch)
        batch.clear()

    async for member_b, score in r.zscan_iter(QUEUE, count=SCAN_COUNT):
        member = member_b.decode()
        if not _is_old_member(member):
            continue
        counter_str, job_json = member.split(":", 1)
        new_member = f"{iso_utc(int(counter_str))}|{job_json}"
        batch.append((member_b, new_member, score))
        if len(batch) >= WRITE_BATCH:
            await flush()

    await flush()
    return migrated


if __name__ == "__main__":
    import os
    import asyncio
    import redis.asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("Set REDIS_URL environment variable")
        exit(-1)

    r = redis.asyncio.Redis.from_url(redis_url)
    count = asyncio.run(fixup_queue(r))
    logging.info("migrated %d queue member(s) to ts|job format", count)
