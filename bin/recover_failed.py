import asyncio
import uvloop
import logging
from urllib.parse import urlparse
from pathlib import Path

import redis.asyncio
import asyncpg

from crawl import make_dispatcher, DEFAULT_MAX_WORKERS, DEFAULT_PRIVATE_KEY_PEM_FILENAME
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph


async def recover_failed(dispatcher, G):
    logging.info("Getting hostnames")
    hostnames = {value async for value in G.node_property_values("hostname")}
    logging.info("Getting failed jobs")
    count = 0
    async for job in dispatcher.failed():
        if job["job_type"] == "page" or job["job_type"] == "collection":
            await dispatcher.enqueue(job)
            await dispatcher.unfail(job)
            count += 1
        elif job["job_type"] == "actor":
            hostname = urlparse(job["actor_id"]).hostname
            if hostname in hostnames:
                await dispatcher.enqueue(job)
                await dispatcher.unfail(job)
                count += 1
    return count


async def main(redis_url, database_url, private_key_pem_filename):

    private_key_pem_data = Path(private_key_pem_filename).read_text()

    r = redis.asyncio.Redis.from_url(redis_url)
    max_conns = max(DEFAULT_MAX_WORKERS // 2, 1)
    pool = await asyncpg.create_pool(
        database_url, max_size=max_conns, min_size=min(max_conns, 10)
    )
    async with pool.acquire() as conn:
        await database_setup(conn)

    try:
        G = DatabaseGraph(pool)
        dispatcher = make_dispatcher(r, G, private_key_pem_data=private_key_pem_data)
        count = await recover_failed(dispatcher, G)
    finally:
        await r.aclose()
        await pool.close()
    return count


if __name__ == "__main__":
    import os
    import sys
    import argparse

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Recover failed jobs if possible.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (env: DATABASE_URL)",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis connection URL (env: REDIS_URL)",
    )
    parser.add_argument(
        "--private-key-pem",
        default=os.environ.get("PRIVATE_KEY_PEM", DEFAULT_PRIVATE_KEY_PEM_FILENAME),
        help="path to the PEM private key file "
        f"(env: PRIVATE_KEY_PEM, default: {DEFAULT_PRIVATE_KEY_PEM_FILENAME})",
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    if not args.redis_url:
        print("Set REDIS_URL environment variable or pass --redis-url")
        sys.exit(1)

    count = asyncio.run(main(args.redis_url, args.database_url, args.private_key_pem))
    print(f"{count} jobs updated")
