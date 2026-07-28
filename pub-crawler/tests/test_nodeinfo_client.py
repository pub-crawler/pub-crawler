"""Tests for nodeinfo_client — fetch and parse a host's nodeinfo document.

One module, two halves:

  NodeinfoClient(general, burst, transport=None, max_workers=50)
    .get_nodeinfo(hostname) -> dict[str, Any] | None
      GET https://{host}/.well-known/nodeinfo (the discovery document), then
      GET the linked schema document, and return parse_nodeinfo() of it —
      the normalized eight-field dict described below. The raw document
      never escapes the client. At most these two requests. Link selection:
      from the discovery doc's `links`, choose the rel
      http://nodeinfo.diaspora.software/ns/schema/{v} with the highest
      supported version, preferring 2.2 > 2.1 > 2.0 > 1.1 > 1.0;
      unrecognized rels are ignored. The href may be relative (resolved against the discovery
      URL) or cross-origin.
      None for any application-level failure: no usable link in the
      discovery doc, or an unparseable (non-JSON) body on either fetch.
      Transport errors and HTTP status errors still raise (raise_for_status
      on both responses). Hostnames are IDNA-encoded (A-label) on the wire.
      Both counters are acquired, keyed by origin, before each of the two
      fetches; the discovery and document origins may differ. Same
      UA/cookie/timeout construction as WebfingerClient.
    .aclose()

  NodeinfoClient.parse_nodeinfo(doc) -> dict  (staticmethod: sync, pure — the
    client's knowledge of the document format, usable without an instance)
    Exactly eight keys, each possibly None: nodeinfo_version, software_name,
    software_version, users_total, users_active_month, users_active_halfyear,
    local_posts, local_comments. Sources: version, software.name,
    software.version, usage.users.total, usage.users.activeMonth,
    usage.users.activeHalfyear, usage.localPosts, usage.localComments.
    Other document members (openRegistrations, protocols, metadata, ...) are
    ignored. Defensive, field-by-field: one bad field never poisons the
    others. Counts accept int, float (rounded), or numeric string;
    booleans, negatives, non-numerics, and absurd values (> 2^62) become
    None. Strings are coerced to str and capped at 128 chars. Missing
    intermediate objects (no usage, no users) yield Nones, never
    exceptions.

Assumed contract (adjust the tests if the shape differs).
"""

import httpx
import pytest

from pub_crawler.nodeinfo_client import NodeinfoClient
from support import SpyCounter, nonblocking_counter

SCHEMA = "http://nodeinfo.diaspora.software/ns/schema/"

MASTODON_DOC = {
    "version": "2.0",
    "software": {"name": "mastodon", "version": "4.3.2"},
    "protocols": ["activitypub"],
    "usage": {
        "users": {"total": 890000, "activeMonth": 230000, "activeHalfyear": 510000},
        "localPosts": 120000000,
    },
    "openRegistrations": True,
}

# parse_nodeinfo(MASTODON_DOC) — also what get_nodeinfo returns when the
# fetched schema document is MASTODON_DOC.
MASTODON_PARSED = {
    "nodeinfo_version": "2.0",
    "software_name": "mastodon",
    "software_version": "4.3.2",
    "users_total": 890000,
    "users_active_month": 230000,
    "users_active_halfyear": 510000,
    "local_posts": 120000000,
    "local_comments": None,  # Mastodon doesn't report comments
}


def discovery(*versions, href=None):
    """A discovery doc linking each version to https://crawler.pub/nodeinfo/{v}
    (or to an explicit href, applied to every link)."""
    return {
        "links": [
            {"rel": SCHEMA + v, "href": href or f"https://crawler.pub/nodeinfo/{v}"}
            for v in versions
        ]
    }


def serve(discovery_doc, docs, seen=None):
    """Handler serving the discovery doc at the well-known path and schema
    documents by path (e.g. "/nodeinfo/2.0"), optionally recording requests."""

    def handler(request):
        if seen is not None:
            seen.append(request)
        if request.url.path == "/.well-known/nodeinfo":
            return httpx.Response(200, json=discovery_doc)
        if request.url.path in docs:
            return httpx.Response(200, json=docs[request.url.path])
        return httpx.Response(404, json={})

    return handler


def make_client(handler, general=None, burst=None):
    return NodeinfoClient(
        general or nonblocking_counter(),
        burst or nonblocking_counter(),
        transport=httpx.MockTransport(handler),
    )


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_get_nodeinfo_returns_the_parsed_fields():
    handler = serve(discovery("2.0"), {"/nodeinfo/2.0": MASTODON_DOC})

    fields = await make_client(handler).get_nodeinfo("crawler.pub")

    assert fields == MASTODON_PARSED


async def test_queries_the_wellknown_discovery_endpoint_over_https():
    seen = []
    handler = serve(discovery("2.0"), {"/nodeinfo/2.0": MASTODON_DOC}, seen)

    await make_client(handler).get_nodeinfo("crawler.pub")

    first = seen[0]
    assert first.url.scheme == "https"
    assert first.url.host == "crawler.pub"
    assert first.url.path == "/.well-known/nodeinfo"
    # Exactly the two GETs: discovery, then the linked document.
    assert [r.url.path for r in seen] == ["/.well-known/nodeinfo", "/nodeinfo/2.0"]


# ---------------------------------------------------------------------------
# Link selection: highest supported version wins, unknown rels are ignored
# ---------------------------------------------------------------------------


async def test_prefers_the_highest_supported_version():
    # 2.2 listed first among lower versions to prove preference, not order.
    handler = serve(
        discovery("2.0", "2.2", "2.1"),
        {
            "/nodeinfo/2.0": MASTODON_DOC,
            "/nodeinfo/2.1": {"version": "2.1"},
            "/nodeinfo/2.2": {"version": "2.2"},
        },
    )

    fields = await make_client(handler).get_nodeinfo("crawler.pub")

    assert fields["nodeinfo_version"] == "2.2"


@pytest.mark.parametrize("v", ["2.2", "2.1", "2.0", "1.1", "1.0"])
async def test_each_supported_version_works_alone(v):
    handler = serve(discovery(v), {f"/nodeinfo/{v}": {"version": v}})

    fields = await make_client(handler).get_nodeinfo("crawler.pub")

    assert fields["nodeinfo_version"] == v


async def test_ignores_unrecognized_rels_when_a_supported_link_exists():
    doc = discovery("2.0")
    doc["links"].insert(
        0, {"rel": "https://example.com/some-other-rel", "href": "https://x.example/"}
    )
    handler = serve(doc, {"/nodeinfo/2.0": MASTODON_DOC})

    assert await make_client(handler).get_nodeinfo("crawler.pub") == MASTODON_PARSED


@pytest.mark.parametrize(
    "doc",
    [
        {},
        {"links": []},
        {"links": [{"rel": "https://example.com/other", "href": "https://x.example/"}]},
    ],
)
async def test_no_usable_link_returns_none(doc):
    assert await make_client(serve(doc, {})).get_nodeinfo("crawler.pub") is None


@pytest.mark.parametrize("path", ["/.well-known/nodeinfo", "/nodeinfo/2.0"])
async def test_unparseable_body_returns_none(path):
    # A 200 with a non-JSON body (usually an HTML error page) on either fetch
    # is an application-level failure, not an exception.
    def handler(request):
        if request.url.path == path:
            return httpx.Response(200, text="<html>oops</html>")
        return httpx.Response(200, json=discovery("2.0", href="/nodeinfo/2.0"))

    assert await make_client(handler).get_nodeinfo("crawler.pub") is None


# ---------------------------------------------------------------------------
# href handling: relative and cross-origin links
# ---------------------------------------------------------------------------


async def test_resolves_relative_href_against_discovery_url():
    seen = []
    handler = serve(
        discovery("2.0", href="/nodeinfo/2.0"), {"/nodeinfo/2.0": MASTODON_DOC}, seen
    )

    fields = await make_client(handler).get_nodeinfo("crawler.pub")

    assert fields == MASTODON_PARSED
    assert seen[1].url.host == "crawler.pub"
    assert seen[1].url.path == "/nodeinfo/2.0"


async def test_follows_cross_origin_href():
    seen = []
    handler = serve(
        discovery("2.0", href="https://files.crawler.pub/ni/2.0"),
        {"/ni/2.0": MASTODON_DOC},
        seen,
    )

    fields = await make_client(handler).get_nodeinfo("crawler.pub")

    assert fields == MASTODON_PARSED
    assert seen[1].url.host == "files.crawler.pub"


# ---------------------------------------------------------------------------
# Errors surface: raise_for_status on both fetches
# ---------------------------------------------------------------------------


async def test_discovery_404_raises_http_status_error():
    def handler(request):
        return httpx.Response(404, json={})

    with pytest.raises(httpx.HTTPStatusError):
        await make_client(handler).get_nodeinfo("crawler.pub")


async def test_linked_document_404_raises_http_status_error():
    handler = serve(discovery("2.0"), {})  # discovery ok, document missing

    with pytest.raises(httpx.HTTPStatusError):
        await make_client(handler).get_nodeinfo("crawler.pub")


# ---------------------------------------------------------------------------
# Real-world quirks: redirects and internationalized hostnames
# ---------------------------------------------------------------------------


async def test_follows_redirect_on_discovery():
    def handler(request):
        if request.url.host == "example.social":
            return httpx.Response(
                302,
                headers={
                    "Location": "https://mastodon.example.social/.well-known/nodeinfo"
                },
            )
        if request.url.path == "/.well-known/nodeinfo":
            return httpx.Response(
                200,
                json=discovery(
                    "2.0", href="https://mastodon.example.social/nodeinfo/2.0"
                ),
            )
        return httpx.Response(200, json=MASTODON_DOC)

    fields = await make_client(handler).get_nodeinfo("example.social")

    assert fields == MASTODON_PARSED


async def test_idn_hostname_is_punycoded_on_the_wire():
    # bawü.social -> xn--baw-joa.social; the A-label must be in the authority.
    punycode = b"xn--baw-joa.social"
    seen = []
    handler = serve(
        discovery("2.0", href="/nodeinfo/2.0"), {"/nodeinfo/2.0": MASTODON_DOC}, seen
    )

    await make_client(handler).get_nodeinfo("bawü.social")

    assert seen[0].url.raw_host == punycode


# ---------------------------------------------------------------------------
# Rate limiting: both counters acquired per origin before each fetch
# ---------------------------------------------------------------------------


async def test_acquires_counters_per_origin_before_each_fetch():
    log = []
    general = SpyCounter(log, "general")
    burst = SpyCounter(log, "burst")
    handler = serve(
        discovery("2.0", href="https://files.crawler.pub/ni/2.0"),
        {"/ni/2.0": MASTODON_DOC},
    )
    client = NodeinfoClient(general, burst, transport=httpx.MockTransport(handler))

    await client.get_nodeinfo("crawler.pub")

    # One acquisition per fetch, keyed by that fetch's origin.
    assert general.calls == ["https://crawler.pub", "https://files.crawler.pub"]
    assert burst.calls == ["https://crawler.pub", "https://files.crawler.pub"]


# ---------------------------------------------------------------------------
# Construction: UA, timeouts, max_workers — same contract as WebfingerClient
# ---------------------------------------------------------------------------


async def test_sends_identifying_user_agent():
    seen = []
    handler = serve(discovery("2.0"), {"/nodeinfo/2.0": MASTODON_DOC}, seen)

    await make_client(handler).get_nodeinfo("crawler.pub")

    for request in seen:
        assert request.headers["User-Agent"].startswith("crawler.pub/")


async def test_timeout_is_patient_read_fast_connect():
    client = make_client(lambda request: httpx.Response(200, json={}))

    assert client.client.timeout == httpx.Timeout(30.0, connect=5.0)


async def test_constructor_accepts_max_workers():
    handler = serve(discovery("2.0"), {"/nodeinfo/2.0": MASTODON_DOC})
    client = NodeinfoClient(
        nonblocking_counter(),
        nonblocking_counter(),
        transport=httpx.MockTransport(handler),
        max_workers=12,
    )

    assert await client.get_nodeinfo("crawler.pub") == MASTODON_PARSED


# ===========================================================================
# parse_nodeinfo — pure parsing, no I/O
# ===========================================================================

EIGHT_KEYS = {
    "nodeinfo_version",
    "software_name",
    "software_version",
    "users_total",
    "users_active_month",
    "users_active_halfyear",
    "local_posts",
    "local_comments",
}


def test_parse_mastodon_20():
    assert NodeinfoClient.parse_nodeinfo(MASTODON_DOC) == MASTODON_PARSED


def test_parse_pleroma_21_with_comments():
    doc = {
        "version": "2.1",
        "software": {"name": "akkoma", "version": "3.13.2"},
        "usage": {
            "users": {"total": 120, "activeMonth": 40, "activeHalfyear": 80},
            "localPosts": 5000,
            "localComments": 900,
        },
        "openRegistrations": False,
    }

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["software_name"] == "akkoma"
    assert parsed["local_comments"] == 900


def test_parse_misskey_null_active_halfyear():
    doc = {
        "version": "2.0",
        "software": {"name": "misskey", "version": "2025.4.0"},
        "usage": {
            "users": {"total": 300, "activeMonth": 50, "activeHalfyear": None},
            "localPosts": 12000,
        },
        "openRegistrations": True,
    }

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["users_active_halfyear"] is None
    assert parsed["users_total"] == 300  # neighbors unaffected


def test_parse_lemmy():
    doc = {
        "version": "2.0",
        "software": {"name": "lemmy", "version": "0.19.5"},
        "usage": {
            "users": {"total": 45000, "activeMonth": 3000, "activeHalfyear": 9000},
            "localPosts": 400000,
            "localComments": 2500000,
        },
        "openRegistrations": True,
    }

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["local_comments"] == 2500000


def test_parse_sparse_usage_writefreely_style():
    doc = {
        "version": "2.0",
        "software": {"name": "writefreely", "version": "0.15.1"},
        "usage": {"users": {"total": 12}},
        "openRegistrations": False,
    }

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["users_total"] == 12
    assert parsed["users_active_month"] is None
    assert parsed["local_posts"] is None


def test_parse_missing_usage_yields_none_counts():
    doc = {"version": "2.0", "software": {"name": "peertube", "version": "6.0"}}

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["software_name"] == "peertube"
    for key in EIGHT_KEYS - {"nodeinfo_version", "software_name", "software_version"}:
        assert parsed[key] is None


def test_parse_missing_software_yields_none_names():
    doc = {"version": "2.0", "usage": {"users": {"total": 5}}}

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["software_name"] is None
    assert parsed["software_version"] is None
    assert parsed["users_total"] == 5


def test_parse_returns_exactly_eight_keys():
    assert set(NodeinfoClient.parse_nodeinfo({}).keys()) == EIGHT_KEYS
    assert set(NodeinfoClient.parse_nodeinfo(MASTODON_DOC).keys()) == EIGHT_KEYS


def test_parse_nodeinfo_version_from_the_doc():
    assert (
        NodeinfoClient.parse_nodeinfo({"version": "2.1"})["nodeinfo_version"] == "2.1"
    )
    assert NodeinfoClient.parse_nodeinfo({})["nodeinfo_version"] is None


# --- count coercion: int passes, float truncates, numeric string parses, ---
# --- garbage becomes None without poisoning neighboring fields           ---


@pytest.mark.parametrize(
    "raw,expected",
    [
        (42, 42),
        (45.9, 46),  # float rounds
        ("123", 123),  # numeric string parses
        (-1, None),  # negative
        (True, None),  # boolean is not a count
        ("many", None),  # non-numeric
        (2**63, None),  # absurdly large
        (None, None),
    ],
)
def test_parse_count_coercion(raw, expected):
    doc = {"usage": {"users": {"total": raw, "activeMonth": 7}}}

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["users_total"] == expected
    assert parsed["users_active_month"] == 7  # never poisoned


def test_parse_caps_string_lengths():
    doc = {"software": {"name": "x" * 500, "version": 42}}

    parsed = NodeinfoClient.parse_nodeinfo(doc)

    assert parsed["software_name"] == "x" * 128  # capped prefix
    assert parsed["software_version"] == "42"  # coerced to str
