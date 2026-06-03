from pathlib import Path
import asyncio
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.webfinger_handler import WebfingerHandler
from pub_crawler.activity_pub_client import ActivityPubClient
from pub_crawler.actor_handler import ActorHandler
import networkx as nx

MAX_WORKERS = 3
MAX_DEPTH = 1
KEY_ID = 'https://crawler.pub/actor'

async def worker(name, q, wfh, ah):
    while True:
        job = await q.get()
        try:
            if job['job_type'] == 'webfinger':
                await wfh.handle(job)
            if job['job_type'] == 'actor':
                await ah.handle(job)
            else:
                raise Exception(f"Unrecognized job type {job['job_type']}")
        except Exception:
            pass
        q.task_done()

async def crawl_graph(inputfile, outputfile, *, transport=None):
    private_key_pem = Path("private.pem").read_text()   # CLI default
    wfc = WebfingerClient(transport=transport)
    ac = ActivityPubClient(KEY_ID, private_key_pem, transport=transport)
    G = nx.DiGraph()
    q = asyncio.Queue()
    wfh = WebfingerHandler(wfc, q, G)
    ah = ActorHandler(ac, q, G, MAX_DEPTH)

    workers = []
    for i in range(MAX_WORKERS):
        workers.append(asyncio.create_task(worker(f'wfw-{i}', q, wfh, ah)))

    try:

        with open(inputfile) as f:
            for line in f:
                wf = line.strip()
                if not wf:
                    continue
                job = {
                    "job_type": "webfinger",
                    "webfinger": wf
                }
                await q.put(job)

        await q.join()

        for w in workers:
            w.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

    finally:
        await wfc.aclose()

    nx.write_gml(G, outputfile)

if __name__ == "__main__":

    import sys
    input = sys.argv[1]
    output = sys.argv[2]
    asyncio.run(crawl_graph(input, output))
