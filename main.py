import asyncio
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
import logging
import redis.asyncio
import asyncpg
from crawler import make_dispatcher
from crawler import crawl_graph
from add_seeds import add_seeds
from snapshot import snapshot

MAX_WORKERS = 25
MAX_DEPTH = 1
KEY_ID = "https://crawler.pub/actor#main-key"


async def main(database_url, redis_url, input_filename, output_filename):

    r = redis.asyncio.Redis.from_url(redis_url)
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)

    try:
        await add_seeds(input_filename, r)
        await crawl_graph(make_dispatcher(r, G))
        await snapshot(G, output_filename)
    finally:
        await pool.close()


if __name__ == "__main__":
    import sys
    import os

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpcore").setLevel(logging.INFO)

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("Set DATABASE_URL environment variable")
        sys.exit(1)

    redis_url = os.environ.get("REDIS_URL")

    if not redis_url:
        print("Set REDIS_URL environment variable")
        sys.exit(1)

    asyncio.run(main(database_url, redis_url, input_filename, output_filename))
