import logging
import redis.asyncio
import asyncio
from pub_crawler.dispatcher import Dispatcher
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.webfinger_handler import WebfingerHandler
from pub_crawler.fixed_window_counter import FixedWindowCounter


async def add_seeds(input_filename, r, *, transport=None):
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    burst = FixedWindowCounter(10, 10 * 1000)
    wfc = WebfingerClient(general, burst, transport=transport)
    dispatcher = Dispatcher(r)
    dispatcher.set_handler("webfinger", WebfingerHandler(wfc, dispatcher, None))

    try:

        with open(input_filename) as f:
            for line in f:
                wf = line.strip()
                if not wf:
                    continue
                job = {"job_type": "webfinger", "webfinger": wf}
                if not await dispatcher.seen(job):
                    await dispatcher.enqueue(job)

    finally:
        await wfc.aclose()


if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpcore").setLevel(logging.INFO)
    import sys

    input_filename = sys.argv[1]

    redis_url = os.environ.get("REDIS_URL")

    if not redis_url:
        print("Set REDIS_URL environment variable")
        exit(-1)

    r = redis.asyncio.Redis.from_url(redis_url)

    asyncio.run(add_seeds(input_filename, r))
