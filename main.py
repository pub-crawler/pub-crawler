from pathlib import Path
import asyncio
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.webfinger_handler import WebfingerHandler
from pub_crawler.activity_pub_client import ActivityPubClient
from pub_crawler.actor_handler import ActorHandler
from pub_crawler.collection_handler import CollectionHandler
from pub_crawler.page_handler import PageHandler
from pub_crawler.fixed_window_counter import FixedWindowCounter
from pub_crawler.dispatcher import Dispatcher
import networkx as nx
import logging

MAX_WORKERS = 25
MAX_DEPTH = 1
KEY_ID = "https://crawler.pub/actor#main-key"


async def worker(name, dispatcher):
    while True:
        job = await dispatcher.get()
        try:
            logging.debug(job)
            await dispatcher.dispatch(job)
        except Exception as e:
            logging.debug(e)
            pass
        dispatcher.done(job)


async def crawl_graph(inputfile, outputfile, *, transport=None):
    private_key_pem = Path("private.pem").read_text()  # CLI default
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    paged = FixedWindowCounter(300, 15 * 60 * 1000)
    wfc = WebfingerClient(general, transport=transport)
    ac = ActivityPubClient(KEY_ID, private_key_pem, general, paged, transport=transport)
    G = nx.DiGraph()
    q = asyncio.PriorityQueue()
    dispatcher = Dispatcher(q)
    dispatcher.set_handler("webfinger", WebfingerHandler(wfc, dispatcher, G))
    dispatcher.set_handler("actor", ActorHandler(ac, dispatcher, G))
    dispatcher.set_handler(
        "collection", CollectionHandler(ac, dispatcher, G, MAX_DEPTH)
    )
    dispatcher.set_handler("page", PageHandler(ac, dispatcher, G))

    workers = []
    for i in range(MAX_WORKERS):
        workers.append(asyncio.create_task(worker(f"wfw-{i}", dispatcher)))

    try:

        with open(inputfile) as f:
            for line in f:
                wf = line.strip()
                if not wf:
                    continue
                job = {"job_type": "webfinger", "webfinger": wf}
                await dispatcher.enqueue(job)

        await q.join()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

    finally:
        await wfc.aclose()
        await ac.aclose()

    nx.write_gml(G, outputfile)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import sys

    input = sys.argv[1]
    output = sys.argv[2]
    asyncio.run(crawl_graph(input, output))
