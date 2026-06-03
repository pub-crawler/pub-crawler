from pathlib import Path
from pub_crawler.webfinger_client import WebfingerClient
from pub_crawler.activity_pub_client import ActivityPubClient
import asyncio

KEY_ID = "https://crawler.pub/actor#main-key"

async def _crawl(id, wf, ap):
    if id.startswith(("http://", "https://")):
        url = id
    else:
        url = await wf.get_actor_id(id)
    return await ap.get(url)

async def crawl(id, *, transport=None, private_key_pem=None):
    if private_key_pem is None:
        private_key_pem = Path("private.pem").read_text()   # CLI default
    wf = WebfingerClient(transport=transport)
    ap = ActivityPubClient(KEY_ID, private_key_pem, transport=transport)
    try:
        return await _crawl(id, wf, ap)
    finally:
        await wf.aclose()
        await ap.aclose()

if __name__ == "__main__":
    import sys
    import json
    arg = sys.argv[1]
    print(json.dumps(asyncio.run(crawl(arg)), indent=2))
