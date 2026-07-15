import logging

import redis.asyncio

from pub_crawler.dispatcher import QUEUE

LOG_INTERVAL = 10_000
SCAN_COUNT = 1000
WRITE_BATCH = 1000


async def fixup_zero_score(r, batch_size=WRITE_BATCH):
    tried = 0
    batch = []
    async for member, _ in r.zscan_iter(QUEUE, count=SCAN_COUNT):
        tried += 1
        if tried % LOG_INTERVAL == 0:
            logging.info(f"{tried} queue jobs seen")
        batch.append(member)
        if len(batch) >= batch_size:
            async with r.pipeline(transaction=False) as pipe:
                for member in batch:
                    pipe.zadd(QUEUE, {member: 0})
                await pipe.execute()
            batch = []
    if len(batch) > 0:
        async with r.pipeline(transaction=False) as pipe:
            for member in batch:
                pipe.zadd(QUEUE, {member: 0})
            await pipe.execute()
        batch = []
    return tried


async def main(redis_url):
    r = redis.asyncio.Redis.from_url(redis_url)

    return await fixup_zero_score(r)


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
