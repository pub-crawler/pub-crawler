import httpx
import orjson
from pub_crawler.block_all_cookies_policy import BlockAllCookiesPolicy
from http.cookiejar import CookieJar
from typing import Any
from urllib.parse import urljoin, urlsplit
import re

DEFAULT_KEEPALIVE_EXPIRY = 10  # Burst window
DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_RESPONSE_TIMEOUT = 30.0


PREFIX = "http://nodeinfo.diaspora.software/ns/schema/"
VERSIONS = ["2.2", "2.1", "2.0", "1.1", "1.0"]
RELS = list(map(lambda v: PREFIX + v, VERSIONS))

MAX_VALUE = 2**62
MAX_STR_LEN = 128


def bounds_int(value):
    if value is None:
        return None
    elif value < 0:
        return None
    elif value > MAX_VALUE:
        return None
    else:
        return value


def coerce_int(value):
    if value is None:
        return None
    elif isinstance(value, bool):
        return None
    elif isinstance(value, int):
        return value
    elif isinstance(value, float):
        return round(value)
    elif isinstance(value, str):
        if re.fullmatch(r"-?\d+", value):
            return int(value)
        else:
            return None
    else:
        return None


def safe_int(value):
    return bounds_int(coerce_int(value))


def coerce_str(value):
    if value is None:
        return None
    elif isinstance(value, str):
        return value
    else:
        return str(value)


def bounds_str(value):
    if value is None:
        return None
    else:
        return value[:MAX_STR_LEN]


def safe_str(value):
    return bounds_str(coerce_str(value))


class NodeinfoClient:

    def __init__(self, general, burst, transport=None, max_workers=50):
        self.general = general
        self.burst = burst
        if transport is None:
            limits = httpx.Limits(
                max_connections=max_workers,
                max_keepalive_connections=max_workers,
                keepalive_expiry=DEFAULT_KEEPALIVE_EXPIRY,
            )
            transport = httpx.AsyncHTTPTransport(http2=True, retries=3, limits=limits)
        self.client = httpx.AsyncClient(
            transport=transport,
            cookies=CookieJar(policy=BlockAllCookiesPolicy()),
            timeout=httpx.Timeout(
                DEFAULT_RESPONSE_TIMEOUT, connect=DEFAULT_CONNECT_TIMEOUT
            ),
        )

    async def get_nodeinfo(self, hostname: str) -> dict[str, Any] | None:
        origin = f"https://{hostname}"
        await self.general.acquire(origin)
        await self.burst.acquire(origin)
        url = f"{origin}/.well-known/nodeinfo"
        headers = {
            "User-Agent": "crawler.pub/0.16.0 (https://crawler.pub/; evanp@gatech.edu)",
            "Accept": "application/json;q=1.0",
        }
        res = await self.client.get(url, headers=headers, follow_redirects=True)
        res.raise_for_status()
        try:
            doc = orjson.loads(res.content)
        except orjson.JSONDecodeError:
            return None
        links = doc.get("links")

        besthref = None
        bestrel = None

        if links is None or not isinstance(links, list):
            return None
        for link in links:
            if not isinstance(link, dict):
                continue
            rel = link.get("rel")
            href = link.get("href")
            if rel is None or href is None:
                continue
            if rel not in RELS:
                continue
            if bestrel is None or rel > bestrel:
                bestrel = rel
                besthref = href

        if besthref is None:
            return None

        nodeinfo_url = urljoin(str(res.url), besthref)
        nodeinfo_url_parts = urlsplit(nodeinfo_url)
        nodeinfo_url_origin = f"https://{nodeinfo_url_parts.netloc}"

        await self.general.acquire(nodeinfo_url_origin)
        await self.burst.acquire(nodeinfo_url_origin)

        res = await self.client.get(
            nodeinfo_url, headers=headers, follow_redirects=True
        )

        res.raise_for_status()
        try:
            doc = orjson.loads(res.content)
        except orjson.JSONDecodeError:
            return None

        return NodeinfoClient.parse_nodeinfo(doc)

    @staticmethod
    def parse_nodeinfo(doc):
        names = [
            "nodeinfo_version",
            "software_name",
            "software_version",
            "users_total",
            "users_active_month",
            "users_active_halfyear",
            "local_posts",
            "local_comments",
        ]
        props = dict()
        for name in names:
            props[name] = None
        if doc is not None and isinstance(doc, dict):
            props["nodeinfo_version"] = safe_str(doc.get("version"))
            software = doc.get("software")
            if software is not None and isinstance(software, dict):
                props["software_name"] = safe_str(software.get("name"))
                props["software_version"] = safe_str(software.get("version"))
            usage = doc.get("usage")
            if usage is not None and isinstance(usage, dict):
                users = usage.get("users")
                if users is not None and isinstance(users, dict):
                    props["users_total"] = safe_int(users.get("total"))
                    props["users_active_month"] = safe_int(users.get("activeMonth"))
                    props["users_active_halfyear"] = safe_int(
                        users.get("activeHalfyear")
                    )
                props["local_posts"] = safe_int(usage.get("localPosts"))
                props["local_comments"] = safe_int(usage.get("localComments"))
        return props

    async def aclose(self):
        await self.client.aclose()
