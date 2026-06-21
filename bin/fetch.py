from pathlib import Path
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.activity_pub_client import ActivityPubClient
from pub_crawler.fixed_window_counter import FixedWindowCounter
import asyncio
import uvloop

KEY_ID = "https://crawler.pub/actor#main-key"


async def _fetch(id, wf, ap):
    if id.startswith(("http://", "https://")):
        url = id
    else:
        url = await wf.get_actor_id(id)
    return await ap.get(url)


async def fetch(id, *, transport=None, private_key_pem=None):
    if private_key_pem is None:
        private_key_pem = Path("private.pem").read_text()  # CLI default
    general = FixedWindowCounter(300, 5 * 60 * 1000)
    paged = FixedWindowCounter(300, 15 * 60 * 1000)
    burst = FixedWindowCounter(10, 10 * 1000)
    wf = WebfingerClient(general, burst, transport=transport)
    ap = ActivityPubClient(
        KEY_ID, private_key_pem, general, paged, burst, transport=transport
    )
    try:
        return await _fetch(id, wf, ap)
    finally:
        await wf.aclose()
        await ap.aclose()


if __name__ == "__main__":
    import sys
    import json

    uvloop.install()

    arg = sys.argv[1]
    print(json.dumps(asyncio.run(fetch(arg)), indent=2))
