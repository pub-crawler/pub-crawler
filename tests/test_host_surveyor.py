"""Tests for host_surveyor — one host's nodeinfo probe, classified and shaped.

  classify_exception(exc) -> str  (sync, pure, module-level)
    Maps an exception from the nodeinfo fetch to a failure string, walking the
    __cause__/__context__ chain (httpx wraps OS-level errors):
      socket.gaierror anywhere in the chain          -> "dns_error"
      ssl.SSLError anywhere in the chain             -> "tls_error"
      httpx.ConnectError / httpx.ConnectTimeout      -> "connect_error"
      other httpx.TimeoutException (read/write/pool) -> "timeout"
      httpx.HTTPStatusError 404/410                  -> "nodeinfo_missing"
      httpx.HTTPStatusError other statuses           -> "http_error"
      ValueError (incl. orjson.JSONDecodeError)      -> "nodeinfo_invalid"
      anything else                                  -> "error"

  HostSurveyor(nodeinfo_client)
    .survey(hostname) -> dict; never raises for per-host problems.
    The dict has a FIXED shape: all eleven survey keys, always present,
    None where this survey produced no value (hostname NOT included — the
    caller knows which host it asked about). The keys:
      last_fetch_date, failure, error_detail, nodeinfo_version,
      software_name, software_version, users_total, users_active_month,
      users_active_halfyear, local_posts, local_comments
    The caller (survey_one) deletes the None-valued names from the store
    and writes the rest — so stale properties from an earlier survey (a
    failure that recovered, software fields of a host that died) are erased
    by the next survey. The surveyor itself never touches the store.
    Calls nodeinfo_client.get_nodeinfo(hostname), which returns the parsed
    eight-field dict (the client runs parse_nodeinfo itself), returns None
    for an application-level failure (no usable link / unparseable body),
    or raises.
      On a dict:      the eight nodeinfo fields carried over (Nones and
                      all); failure and error_detail are None — absence of
                      a stored failure IS success.
      On None:        failure "nodeinfo_invalid"; error_detail None.
      On exception:   failure = classify_exception(exc), error_detail =
                      repr(exc) truncated to <= 500 chars; nodeinfo fields
                      all None.
    There is no separate stage property: the failure taxonomy already
    locates where the probe stopped (dns_error / connect_error / tls_error
    are transport-dead; nodeinfo_* and http_error mean the host is alive).
    last_fetch_date is never None: an ISO-8601 UTC timestamp string, set at
    survey time, on failures too (failed hosts wait out --max-age).

Assumed contract (adjust the tests if the shape differs).
"""

import socket
import ssl
from datetime import datetime, timezone

import httpx
import pytest

from pub_crawler.host_surveyor import HostSurveyor, classify_exception

SURVEY_KEYS = {
    "last_fetch_date",
    "failure",
    "error_detail",
    "nodeinfo_version",
    "software_name",
    "software_version",
    "users_total",
    "users_active_month",
    "users_active_halfyear",
    "local_posts",
    "local_comments",
}

# What get_nodeinfo hands back for a healthy Mastodon box: parse_nodeinfo's
# normalized eight-field shape, not a raw document.
NODEINFO_FIELDS = {
    "nodeinfo_version": "2.0",
    "software_name": "mastodon",
    "software_version": "4.3.2",
    "users_total": 890000,
    "users_active_month": 230000,
    "users_active_halfyear": 510000,
    "local_posts": 120000000,
    "local_comments": None,
}


class FakeNodeinfoClient:
    """get_nodeinfo returns a canned parsed-fields dict (None included — the
    client's application-level-failure signal) or raises a canned error."""

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error

    async def get_nodeinfo(self, hostname):
        if self.error is not None:
            raise self.error
        return self.result


def status_error(code):
    """An httpx.HTTPStatusError as raise_for_status would produce."""
    request = httpx.Request("GET", "https://crawler.pub/.well-known/nodeinfo")
    response = httpx.Response(code, request=request)
    return httpx.HTTPStatusError(f"status {code}", request=request, response=response)


def chained(outer, *inner):
    """outer with the inner exceptions attached as a __cause__/__context__
    chain, mimicking httpx's wrapping of OS-level errors (the interesting
    error is often two links down, and on the __context__ side)."""
    exc = outer
    for i, cause in enumerate(inner):
        if i % 2 == 0:
            exc.__cause__ = cause
        else:
            exc.__context__ = cause
        exc = cause
    return outer


async def survey(client):
    return await HostSurveyor(client).survey("crawler.pub")


# ---------------------------------------------------------------------------
# classify_exception: chain-walking and type mapping
# ---------------------------------------------------------------------------


def test_gaierror_in_cause_chain_is_dns_error():
    exc = chained(
        httpx.ConnectError("failed"),
        socket.gaierror(8, "nodename nor servname provided"),
    )
    assert classify_exception(exc) == "dns_error"


def test_ssl_error_two_links_down_is_tls_error():
    # httpx.ConnectError -> OSError -> ssl.SSLCertVerificationError, crossing
    # from __cause__ to __context__: the walk must follow both attributes.
    exc = chained(
        httpx.ConnectError("failed"),
        OSError("handshake"),
        ssl.SSLCertVerificationError(1, "certificate verify failed"),
    )
    assert classify_exception(exc) == "tls_error"


def test_bare_connect_error_is_connect_error():
    assert classify_exception(httpx.ConnectError("refused")) == "connect_error"


def test_connect_timeout_is_connect_error():
    # ConnectTimeout is also a TimeoutException; the connect side must win.
    assert classify_exception(httpx.ConnectTimeout("timed out")) == "connect_error"


def test_read_timeout_is_timeout():
    assert classify_exception(httpx.ReadTimeout("timed out")) == "timeout"


def test_unwrapped_gaierror_and_ssl_error_classify_directly():
    assert classify_exception(socket.gaierror(8, "no address")) == "dns_error"
    assert classify_exception(ssl.SSLError(1, "protocol violation")) == "tls_error"


@pytest.mark.parametrize("code", [404, 410])
def test_status_404_and_410_are_nodeinfo_missing(code):
    assert classify_exception(status_error(code)) == "nodeinfo_missing"


@pytest.mark.parametrize("code", [403, 500, 503])
def test_other_statuses_are_http_error(code):
    assert classify_exception(status_error(code)) == "http_error"


def test_value_error_is_nodeinfo_invalid():
    # orjson.JSONDecodeError subclasses ValueError, so both routes land here.
    assert classify_exception(ValueError("no usable link")) == "nodeinfo_invalid"


def test_anything_else_is_error():
    assert classify_exception(RuntimeError("surprise")) == "error"


# ---------------------------------------------------------------------------
# survey: success merges fields, failure classifies, nothing ever raises.
# The returned dict is the set_host_properties payload.
# ---------------------------------------------------------------------------


async def test_success_has_the_nodeinfo_fields_and_none_failure():
    result = await survey(FakeNodeinfoClient(result=NODEINFO_FIELDS))

    assert set(result.keys()) == SURVEY_KEYS  # fixed shape, hostname excluded
    assert result["nodeinfo_version"] == "2.0"
    assert result["software_name"] == "mastodon"
    assert result["software_version"] == "4.3.2"
    assert result["users_total"] == 890000
    assert result["users_active_month"] == 230000
    assert result["local_posts"] == 120000000
    # Unknowns are explicit Nones — the caller deletes these from the store
    assert result["failure"] is None
    assert result["error_detail"] is None
    assert result["local_comments"] is None  # Mastodon reports no comments


@pytest.mark.parametrize(
    "error,failure",
    [
        (chained(httpx.ConnectError("x"), socket.gaierror(8, "no addr")), "dns_error"),
        (httpx.ConnectError("refused"), "connect_error"),
        (httpx.ReadTimeout("slow"), "timeout"),
        (
            chained(httpx.ConnectError("x"), ssl.SSLCertVerificationError(1, "bad")),
            "tls_error",
        ),
        (status_error(404), "nodeinfo_missing"),
        (status_error(500), "http_error"),
        (ValueError("no usable link"), "nodeinfo_invalid"),
        (RuntimeError("surprise"), "error"),
    ],
)
async def test_failures_classify_into_failure(error, failure):
    result = await survey(FakeNodeinfoClient(error=error))

    assert set(result.keys()) == SURVEY_KEYS  # fixed shape on failure too
    assert result["failure"] == failure
    assert result["error_detail"]  # non-empty
    assert result["software_name"] is None  # nothing parsed
    assert result["users_total"] is None


async def test_none_from_the_client_is_nodeinfo_invalid():
    # get_nodeinfo's application-level-failure signal: no usable link, or an
    # unparseable body. No exception to detail, just the classification.
    result = await survey(FakeNodeinfoClient(result=None))

    assert result["failure"] == "nodeinfo_invalid"
    assert result["error_detail"] is None  # no exception to describe
    assert result["software_name"] is None


async def test_http_error_detail_carries_the_status():
    result = await survey(FakeNodeinfoClient(error=status_error(500)))

    assert "500" in result["error_detail"]


async def test_error_detail_is_truncated():
    result = await survey(FakeNodeinfoClient(error=RuntimeError("x" * 2000)))

    assert len(result["error_detail"]) <= 500


async def test_survey_never_raises():
    # Even an exception type the taxonomy has never heard of comes back as a
    # result, not a raise. Exception-derived only: BaseExceptions like
    # asyncio.CancelledError MUST still propagate, or survey tasks would be
    # uncancellable — so the implementation should catch Exception, no wider.
    class Novel(Exception):
        pass

    result = await survey(FakeNodeinfoClient(error=Novel()))

    assert result["failure"] == "error"


# ---------------------------------------------------------------------------
# last_fetch_date: ISO-8601 UTC, stamped on success and failure alike
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "client",
    [
        FakeNodeinfoClient(result=NODEINFO_FIELDS),
        FakeNodeinfoClient(error=httpx.ConnectError("refused")),
        FakeNodeinfoClient(result=None),
    ],
)
async def test_every_result_is_stamped_with_utc_last_fetch_date(client):
    result = await survey(client)

    stamped = datetime.fromisoformat(result["last_fetch_date"])
    assert stamped.utcoffset().total_seconds() == 0
    assert abs((datetime.now(timezone.utc) - stamped).total_seconds()) < 60
