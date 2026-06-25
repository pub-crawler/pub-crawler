import logging
import redis.asyncio
import asyncio
import uvloop
from pub_crawler.dispatcher import Dispatcher
from pub_crawler.fixed_window_counter import FixedWindowCounter
from pub_crawler.activity_pub_client import ActivityPubClient
from pub_crawler.actor_handler import ActorHandler
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
import anyio
import asyncpg

DEFAULT_KEY_ID = "https://crawler.pub/actor#main-key"
DEFAULT_PRIVATE_KEY_PEM_FILENAME = "private.pem"


async def add_seeds_by_actor_id(
    input_filename,
    r,
    G,
    *,
    key_id=DEFAULT_KEY_ID,
    private_key_pem_data,
    transport=None
):
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    paged = FixedWindowCounter(300, 15 * 60 * 1000)
    burst = FixedWindowCounter(10, 10 * 1000)
    ac = ActivityPubClient(
        key_id,
        private_key_pem_data,
        general,
        paged,
        burst,
        transport=transport,
        max_workers=1,
    )
    dispatcher = Dispatcher(r)
    dispatcher.set_handler("actor", ActorHandler(ac, dispatcher, G))

    try:

        with open(input_filename) as f:
            for line in f:
                actor_id = line.strip()
                if not actor_id:
                    continue
                job = {"job_type": "actor", "actor_id": actor_id, "depth": 0}
                if not await dispatcher.seen(job):
                    await dispatcher.enqueue(job)

    finally:
        await ac.aclose()


async def main(input_filename, key_id, private_key_pem_file, redis_url, database_url):
    r = redis.asyncio.Redis.from_url(redis_url)
    max_conns = 1
    pool = await asyncpg.create_pool(
        database_url, max_size=max_conns, min_size=min(max_conns, 10)
    )
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)
    private_key_pem_data = await anyio.Path(private_key_pem_file).read_text()
    await add_seeds_by_actor_id(
        input_filename,
        r,
        G,
        key_id=key_id,
        private_key_pem_data=private_key_pem_data,
    )


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

    parser = argparse.ArgumentParser(description="Add actor IDs from a seed file")
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
        "--key-id",
        default=os.environ.get("KEY_ID", DEFAULT_KEY_ID),
        help=f"HTTP Signature key id (env: KEY_ID, default: {DEFAULT_KEY_ID})",
    )
    parser.add_argument(
        "--private-key-pem",
        default=os.environ.get("PRIVATE_KEY_PEM", DEFAULT_PRIVATE_KEY_PEM_FILENAME),
        help="path to the PEM private key file "
        f"(env: PRIVATE_KEY_PEM, default: {DEFAULT_PRIVATE_KEY_PEM_FILENAME})",
    )

    parser.add_argument("input_filename")

    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    if not args.redis_url:
        print("Set REDIS_URL environment variable or pass --redis-url")
        sys.exit(1)

    asyncio.run(
        main(
            args.input_filename,
            args.key_id,
            args.private_key_pem,
            args.redis_url,
            args.database_url,
        )
    )
