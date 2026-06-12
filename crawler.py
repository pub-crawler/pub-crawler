from pathlib import Path
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.webfinger_handler import WebfingerHandler
from pub_crawler.activity_pub_client import ActivityPubClient
from pub_crawler.actor_handler import ActorHandler
from pub_crawler.collection_handler import CollectionHandler
from pub_crawler.page_handler import PageHandler
from pub_crawler.fixed_window_counter import FixedWindowCounter
from pub_crawler.dispatcher import Dispatcher
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
import logging
import asyncio
import redis.asyncio
import asyncpg

KEY_ID = "https://crawler.pub/actor#main-key"
MAX_DEPTH = 1
MAX_WORKERS = 25


def make_dispatcher(
    redis,
    G,
    *,
    transport=None,
    key_id=KEY_ID,
    private_key_pem=None,
    max_depth=MAX_DEPTH,
):
    if private_key_pem is None:
        private_key_pem = Path("private.pem").read_text()  # CLI default
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    paged = FixedWindowCounter(300, 15 * 60 * 1000)
    wfc = WebfingerClient(general, transport=transport)
    ac = ActivityPubClient(key_id, private_key_pem, general, paged, transport=transport)
    dispatcher = Dispatcher(redis)
    dispatcher.set_handler("webfinger", WebfingerHandler(wfc, dispatcher, G))
    dispatcher.set_handler("actor", ActorHandler(ac, dispatcher, G))
    dispatcher.set_handler(
        "collection", CollectionHandler(ac, dispatcher, G, max_depth)
    )
    dispatcher.set_handler("page", PageHandler(ac, dispatcher, G))
    return dispatcher


async def worker(name, dispatcher):
    while True:
        job = await dispatcher.get()
        try:
            logging.debug(job)
            await dispatcher.dispatch(job)
        except Exception as e:
            logging.warning(e)
            pass
        await dispatcher.done(job)


async def crawl_graph(dispatcher, *, max_workers=MAX_WORKERS):

    workers = []
    for i in range(max_workers):
        workers.append(asyncio.create_task(worker(f"wfw-{i}", dispatcher)))

    await dispatcher.join()

    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, return_exceptions=True)


async def main(redis_url, database_url):

    r = redis.asyncio.Redis.from_url(redis_url)
    pool = await asyncpg.create_pool(database_url)
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)

    try:
        await crawl_graph(make_dispatcher(r, G))
    finally:
        await pool.close()


if __name__ == "__main__":
    import os
    import sys

    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("Set DATABASE_URL environment variable")
        sys.exit(1)

    redis_url = os.environ.get("REDIS_URL")

    if not redis_url:
        print("Set REDIS_URL environment variable")
        sys.exit(1)

    asyncio.run(main(redis_url, database_url))
