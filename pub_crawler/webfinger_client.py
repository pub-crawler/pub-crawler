import httpx
from email.utils import formatdate
from urllib.parse import urlsplit

MEDIA_TYPES = [
    "application/activity+json",
    'application/ld+json; profile="https://www.w3.org/ns/activitystreams"',
]


class WebfingerClient:
    def __init__(self, general, burst, transport=None):
        self.general = general
        self.burst = burst
        if transport is None:
            transport = httpx.AsyncHTTPTransport(retries=3)
        self.client = httpx.AsyncClient(transport=transport)

    async def get_actor_id(self, wf):
        resource = self._normalize(wf)
        hostname = wf.split("@")[-1]
        origin = f"https://{hostname}"
        url = f"{origin}/.well-known/webfinger?resource={resource}"
        headers = {
            "User-Agent": "crawler.pub/0.4.1 (https://crawler.pub/; evanp@gatech.edu)",
            "Accept": "application/jrd+json;q=1.0,application/json;q=0.5",
        }
        await self.general.acquire(origin)
        await self.burst.acquire(origin)
        res = await self.client.get(url, headers=headers)
        res.raise_for_status()
        doc = res.json()
        if doc["subject"] != resource:
            raise Exception(
                f"Webfinger subject {doc["subject"]} does not match {resource}"
            )
        for media_type in MEDIA_TYPES:
            for link in doc["links"]:
                if link.get("type") == media_type and link.get("rel") == "self":
                    return link["href"]
        raise ValueError(f"no actor link for {resource}")

    async def aclose(self):
        await self.client.aclose()

    def next_available(self, wf):
        hostname = wf.split("@")[-1]
        origin = f"https://{hostname}"
        return max(
            self.general.next_available(origin), self.burst.next_available(origin)
        )

    def _normalize(self, wf):
        if wf[0] == "@":
            return "acct:" + wf[1:]
        elif wf.startswith("acct:"):
            return wf
        else:
            return "acct:" + wf
