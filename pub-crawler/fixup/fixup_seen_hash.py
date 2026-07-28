import logging

import redis.asyncio
import hashlib

from pub_crawler.dispatcher import SEEN, SEEN_HASHED

LOG_INTERVAL = 50_000
SCAN_COUNT = 50_000
WRITE_BATCH = 50_000


async def write_batch(r, batch):
    async with r.pipeline(transaction=False) as pipe:
        for jid in batch:
            hash = hashlib.blake2b(jid, digest_size=8).digest()
            pipe.sadd(SEEN_HASHED, hash)
            pipe.srem(SEEN, jid)
        await pipe.execute()


async def fixup_seen_hash(r, batch_size=WRITE_BATCH):
    tried = 0
    batch = []
    async for jid in r.sscan_iter(SEEN, count=SCAN_COUNT):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} seen jobs read")
        batch.append(jid)
        if len(batch) >= batch_size:
            await write_batch(r, batch)
            batch = []
    if len(batch) > 0:
        await write_batch(r, batch)
        batch = []
    return tried


async def main(redis_url):
    r = redis.asyncio.Redis.from_url(redis_url)

    return await fixup_seen_hash(r)


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

    print(f"{tried} total seen jobs")
