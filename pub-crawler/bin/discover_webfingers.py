import logging
import asyncio
import uvloop
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.fixed_window_counter import FixedWindowCounter


async def discover_webfingers(input_filename, output_filename, *, transport=None):
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    burst = FixedWindowCounter(10, 10 * 1000)
    wfc = WebfingerClient(general, burst, transport=transport)

    try:

        with open(input_filename) as f:
            with open(output_filename, "w", buffering=1) as g:
                g.write("webfinger,actor_id\n")
                for line in f:
                    wf = line.strip()
                    if not wf:
                        continue
                    try:
                      id = await wfc.get_actor_id(wf)
                      if id:
                          g.write(wf + "," + id + "\n")
                    except Exception as e:
                        print(wf, ": ", e)
                        continue

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
