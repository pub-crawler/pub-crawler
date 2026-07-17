import logging
import asyncio
import uvloop
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.fixed_window_counter import FixedWindowCounter
from pub_crawler.throttle import (
    BURST_LIMIT,
    BURST_WINDOW,
    GENERAL_LIMIT,
    GENERAL_WINDOW,
)

sem = asyncio.Semaphore(25)


async def resolve_one(wf, wfc):
    async with sem:
        try:
            id = await wfc.get_actor_id(wf)
            if id:
                logging.info(f"{wf} -> {id}")
                return (wf, id)
        except Exception as e:
            logging.warning(f"{wf}: {e!r}")
            return None


async def discover_webfingers(input_filename, output_filename, *, transport=None):
    general = FixedWindowCounter(GENERAL_LIMIT, GENERAL_WINDOW)
    burst = FixedWindowCounter(BURST_LIMIT, BURST_WINDOW)
    wfc = WebfingerClient(general, burst, transport=transport)

    webfingers = []

    try:

        with open(input_filename) as f:
            for line in f:
                wf = line.strip()
                if not wf or wf == "webfinger":
                    continue
                webfingers.append(wf)

        pairs = await asyncio.gather(*(resolve_one(wf, wfc) for wf in webfingers))

        with open(output_filename, "w", buffering=1) as g:
            g.write("webfinger,actor_id\n")
            for pair in pairs:
                if pair is None:
                    continue
                g.write(pair[0] + "," + pair[1] + "\n")
    finally:
        await wfc.aclose()


if __name__ == "__main__":
    import os
    import sys

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)
    import sys

    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    asyncio.run(discover_webfingers(input_filename, output_filename))
