import logging

import redis.asyncio
import orjson
from zlib import crc32

from pub_crawler.dispatcher import QUEUE
from pub_crawler.job_id import job_id

LOG_INTERVAL = 10_000
SCAN_COUNT = 1000
WRITE_BATCH = 1000


def is_old_member(member):
    return "T" in member.split("|", 3)[2]


async def write_batch(r, batch):
    async with r.pipeline(transaction=False) as pipe:
        for member in batch:
            depth, job_type_code, ts, jobstr = member.split("|", 3)
            job = orjson.loads(jobstr)
            jid = job_id(job)
            if jid is None:
                logging.warning(f"job with no jid: {jobstr}")
                continue
            crc = crc32(jid.encode())
            newmember = f"{depth}|{job_type_code}|{crc:08x}|{ts}|{jobstr}"
            pipe.zadd(QUEUE, {newmember: 0})
            pipe.zrem(QUEUE, member)
        await pipe.execute()


async def fixup_crc_tiebreak(r, batch_size=WRITE_BATCH):
    tried = 0
    batch = []
    async for memberb, _ in r.zscan_iter(QUEUE, count=SCAN_COUNT):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} queue jobs seen")
        member = memberb.decode()
        if is_old_member(member):
            batch.append(member)
        if len(batch) >= batch_size:
            await write_batch(r, batch)
            batch = []
    if len(batch) > 0:
        await write_batch(r, batch)
        batch = []
    return tried


async def main(redis_url):
    r = redis.asyncio.Redis.from_url(redis_url)

    return await fixup_crc_tiebreak(r)


if __name__ == "__main__":
    import os
    import asyncio

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("Set REDIS_URL environment variable")
        exit(-1)

    tried = asyncio.run(main(redis_url))

    print(f"{tried} total queue items")
