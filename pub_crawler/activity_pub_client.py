from pub_crawler.signature import signature_header
import httpx
from email.utils import formatdate
from urllib.parse import urlsplit, urljoin, parse_qs

MAX_RECURSIONS = 20
ACCEPT = (
    "application/activity+json;q=1.0,application/ld+json;q=0.8,application/json;q=0.5"
)


class ActivityPubClient:

    def __init__(self, key_id, private_key_pem, general, paged, burst, transport=None):
        self.key_id = key_id
        self.private_key_pem = private_key_pem
        self.general = general
        self.paged = paged
        self.burst = burst
        if transport is None:
            transport = httpx.AsyncHTTPTransport(retries=3)
        self.client = httpx.AsyncClient(transport=transport)

    async def get(self, url):
        json, _ = await self._get_with_headers(url, MAX_RECURSIONS)
        return json

    async def get_with_headers(self, url):
        return await self._get_with_headers(url, MAX_RECURSIONS)

    async def aclose(self):
        await self.client.aclose()

    def next_available(self, url):
        parts = urlsplit(url)
        host = parts.netloc
        origin = f"https://{host}"
        if parts.query and "page" in parse_qs(parts.query):
            return max(
                self.paged.next_available(origin),
                self.general.next_available(origin),
                self.burst.next_available(origin)
            )
        else:
            return max(
                self.general.next_available(origin),
                self.burst.next_available(origin)
            )

    async def _get_with_headers(self, url, recursions_left):
        parts = urlsplit(url)
        host = parts.netloc
        origin = f"https://{host}"
        to_sign = {
            "Date": formatdate(usegmt=True),
            "Host": urlsplit(url).netloc,
            "User-Agent": "crawler.pub/0.3.2 (https://crawler.pub/; evanp@gatech.edu)",
        }
        signature = signature_header(
            url, "GET", to_sign, self.key_id, self.private_key_pem
        )
        headers = {**to_sign, "Signature": signature, "Accept": ACCEPT}
        if parts.query and "page" in parse_qs(parts.query):
            await self.paged.acquire(origin)
        await self.general.acquire(origin)
        await self.burst.acquire(origin)
        response = await self.client.get(url, headers=headers)
        if 300 <= response.status_code < 400:
            if recursions_left <= 0:
                raise httpx.TooManyRedirects("Too many redirects")
            return await self._get_with_headers(
                urljoin(url, response.headers["Location"]), recursions_left - 1
            )
        response.raise_for_status()
        return response.json(), response.headers
