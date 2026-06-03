from pathlib import Path
from webfinger import WebfingerClient
from client import ActivityPubClient

KEY_ID = "https://crawler.pub/actor#main-key"

def _crawl(id, wf, ap):
    if id.startswith(("http://", "https://")):
        url = id
    else:
        url = wf.get_actor_id(id)
    return ap.get(url)

def crawl(id, *, transport=None, private_key_pem=None):
    if private_key_pem is None:
        private_key_pem = Path("private.pem").read_text()   # CLI default
    wf = WebfingerClient(transport=transport)
    ap = ActivityPubClient(KEY_ID, private_key_pem, transport=transport)
    return _crawl(id, wf, ap)

if __name__ == "__main__":
    import sys
    import json
    arg = sys.argv[1]
    print(json.dumps(crawl(arg), indent=2))
