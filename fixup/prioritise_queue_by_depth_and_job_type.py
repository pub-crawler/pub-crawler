import logging
import re
from pub_crawler.dispatcher import QUEUE
import json

SCAN_COUNT = 1000  # ZSCAN per-call batch hint (fewer round-trips than the default)
WRITE_BATCH = 1000  # members rewritten per pipeline


def _is_old_member(member):
    return not re.match(r"\d{2}\|", member)


async def flush(r, batch):
    migrated = 0
    if not batch:
        return migrated
    async with r.pipeline(transaction=True) as pipe:
        for old_b, new_member, score in batch:
            pipe.zrem(QUEUE, old_b)
            pipe.zadd(QUEUE, {new_member: score})
        await pipe.execute()
    migrated += len(batch)
    batch.clear()
    return migrated


async def prioritise_queue_by_depth_and_job_type(r):
    """Rewrite every old-format member of the QUEUE ZSET in place.

    Returns the number of members migrated. Idempotent: re-running skips members
    already in `ts|job` form, so it is safe to run again if interrupted.
    """
    migrated = 0
    batch = []  # (old_member_bytes, new_member_str, score) awaiting a flush

    async for member_b, score in r.zscan_iter(QUEUE, count=SCAN_COUNT):
        member = member_b.decode()
        if not _is_old_member(member):
            continue
        ts, job_json = member.split("|", 1)
        job = json.loads(job_json)
        depth = job.get("depth", 0)
        job_type = job.get("job_type")
        if job_type == "webfinger":
            job_type_code = 10
        elif job_type == "actor":
            job_type_code = 20
        elif job_type == "collection":
            job_type_code = 30
        elif job_type == "page":
            job_type_code = 40
        else:
            raise Exception(f"unrecognized job type {job_type}")
        depth = max(min(depth, 99), 0)
        job_type_code = max(min(job_type_code, 99), 0)
        new_member = f"{depth:02d}|{job_type_code:02d}|{ts}|{json.dumps(job, sort_keys=True)}"
        batch.append((member_b, new_member, score))
        if len(batch) >= WRITE_BATCH:
            migrated += await flush(r, batch)
            logging.info(f"{migrated} jobs migrated")

    migrated += await flush(r, batch)
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
    count = asyncio.run(prioritise_queue_by_depth_and_job_type(r))
    logging.info("migrated %d queue member(s) to depth|job type|ts|job format", count)
