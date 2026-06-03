from pub_crawler.signature import signature_header
import httpx
from email.utils import formatdate
from urllib.parse import urlsplit, urljoin

MAX_RECURSIONS=20
ACCEPT="application/activity+json;q=1.0,application/ld+json;q=0.8,application/json;q=0.5"

class ActivityPubClient:

  def __init__(self, key_id, private_key_pem, transport=None):
    self.key_id = key_id
    self.private_key_pem = private_key_pem
    self.client = httpx.AsyncClient(transport=transport)

  async def get(self, url):
    return await self._get(url, MAX_RECURSIONS)

  async def aclose(self):
    await self.client.aclose()

  async def _get(self, url, recursions_left):
    to_sign = {
      "Date": formatdate(usegmt=True),
      "Host": urlsplit(url).netloc,
      "User-Agent": "crawler.pub/0.1.0 (https://crawler.pub/; evanp@gatech.edu)"
    }
    signature = signature_header(url, 'GET', to_sign, self.key_id, self.private_key_pem)
    headers = {**to_sign, "Signature": signature, "Accept": ACCEPT}
    response = await self.client.get(url, headers=headers)
    if 300 <= response.status_code < 400:
      if recursions_left <= 0:
        raise httpx.TooManyRedirects('Too many redirects')
      return await self._get(urljoin(url, response.headers["Location"]), recursions_left-1)
    response.raise_for_status()
    return response.json()
