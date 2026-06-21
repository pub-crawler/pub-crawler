import asyncio
from pathlib import Path
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
from pub_crawler.crawler import Crawler
import logging
import redis.asyncio
import asyncpg
from crawl import (
    make_dispatcher,
    DEFAULT_KEY_ID,
    DEFAULT_PRIVATE_KEY_PEM_FILENAME,
    DEFAULT_MAX_DEPTH,
    DEFAULT_MAX_WORKERS,
)
from add_seeds import add_seeds
from snapshot import snapshot
import signal
import uvloop


async def main(
    redis_url,
    database_url,
    input_filename,
    output_filename,
    *,
    max_depth=DEFAULT_MAX_DEPTH,
    max_workers=DEFAULT_MAX_WORKERS,
    key_id=DEFAULT_KEY_ID,
    private_key_pem_filename=DEFAULT_PRIVATE_KEY_PEM_FILENAME,
):
    private_key_pem_data = Path(private_key_pem_filename).read_text()

    r = redis.asyncio.Redis.from_url(redis_url)
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)
    shutdown = asyncio.Event()

    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, shutdown.set)

    try:
        await add_seeds(input_filename, r)
        dispatcher = make_dispatcher(
            r,
            G,
            key_id=key_id,
            private_key_pem_data=private_key_pem_data,
            max_depth=max_depth,
            max_workers=max_workers
        )
        crawler = Crawler(dispatcher, max_workers)
        await crawler.start()
        finish_task = asyncio.create_task(crawler.finish())
        shutdown_task = asyncio.create_task(shutdown.wait())
        done, pending = await asyncio.wait(
            {finish_task, shutdown_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if shutdown_task in done:
            await crawler.abort()

        for other in pending:
            other.cancel()
            try:
                await other
            except asyncio.CancelledError:
                pass

        if finish_task in done:
            await snapshot(G, output_filename)
    finally:
        await pool.close()
        await r.aclose()


if __name__ == "__main__":
    import sys
    import os
    import argparse

    uvloop.install()

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpcore").setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        description="Seed, crawl the Fediverse follower/following graph, and snapshot it to GML."
    )
    parser.add_argument("input_filename", help="file of seed acct/actor identifiers")
    parser.add_argument("output_filename", help="path to write the GML snapshot")
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
    parser.add_argument(
        "--max-depth",
        type=int,
        default=int(os.environ.get("MAX_DEPTH", DEFAULT_MAX_DEPTH)),
        help="how many hops out from the seed to follow "
        f"(env: MAX_DEPTH, default: {DEFAULT_MAX_DEPTH})",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("MAX_WORKERS", DEFAULT_MAX_WORKERS)),
        help="number of concurrent worker tasks "
        f"(env: MAX_WORKERS, default: {DEFAULT_MAX_WORKERS})",
    )
    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    if not args.redis_url:
        print("Set REDIS_URL environment variable or pass --redis-url")
        sys.exit(1)

    asyncio.run(
        main(
            args.redis_url,
            args.database_url,
            args.input_filename,
            args.output_filename,
            max_depth=args.max_depth,
            max_workers=args.max_workers,
            key_id=args.key_id,
            private_key_pem_filename=args.private_key_pem,
        )
    )
