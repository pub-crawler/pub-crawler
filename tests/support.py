"""Shared constants and helpers for the test suite."""

import base64
import re
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

from pub_crawler.fixed_window_counter import FixedWindowCounter

AS2_CONTEXT = "https://www.w3.org/ns/activitystreams"
SECURITY_CONTEXT = "https://w3id.org/security/v1"
FEP_5711_CONTEXT = "https://w3id.org/fep/5711"
WEBFINGER_CONTEXT = "https://purl.archive.org/socialweb/webfinger"

AS2_TYPE = "application/activity+json"
LD_JSON_TYPE = "application/ld+json"
JRD_TYPE = "application/jrd+json"

# Actor collection property -> FEP-5711 inverse property (each Functional,
# each pointing back at the actor id).
COLLECTIONS = {
    "inbox": "inboxOf",
    "outbox": "outboxOf",
    "followers": "followersOf",
    "following": "followingOf",
    "liked": "likedOf",
}


def media_type(response):
    """The bare media type of a response, without parameters or charset."""
    return response.headers["content-type"].split(";")[0].strip().lower()


def as_list(value):
    """Normalise a JSON-LD value that may be a scalar or a list into a list."""
    if isinstance(value, list):
        return value
    return [value]


# --- HTTP Signature (draft-cavage-12) verification helpers ------------------


def parse_signature(header_value):
    """Parse a Signature header value into its keyId/algorithm/headers/signature."""
    return dict(re.findall(r'(\w+)="([^"]*)"', header_value))


def canonical_signing_string(method, url, headers):
    """The draft-cavage-12 signing string a verifier independently reconstructs.

    `headers` is iterated in order; build it in the order the Signature's
    `headers=` field declares so the reconstruction matches what was signed.
    """
    parts = urlsplit(url)
    target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    lines = [f"(request-target): {method.lower()} {target}"]
    for name, value in headers.items():
        lines.append(f"{name.lower()}: {value}")
    return "\n".join(lines)


def verify_signature(public_key, signature_b64, message):
    """Raise cryptography's InvalidSignature if signature doesn't match message."""
    public_key.verify(
        base64.b64decode(signature_b64),
        message.encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )


# --- Rate-limit counters for client tests -----------------------------------


def nonblocking_counter():
    """A real FixedWindowCounter with a budget large enough never to block in a
    test, so existing assertions are unaffected by the acquire() call."""
    return FixedWindowCounter(10_000, 60_000)


class SpyCounter:
    """Stand-in for FixedWindowCounter that records acquire(origin) calls.

    `log`, if given, is a shared list onto which each call appends
    (label, origin) — so a test can assert ordering against other events
    (e.g. that acquire happens before the fetch, or paged before general).
    """

    def __init__(self, log=None, label="acquire"):
        self.calls = []
        self.log = log
        self.label = label

    async def acquire(self, origin):
        self.calls.append(origin)
        if self.log is not None:
            self.log.append((self.label, origin))


# --- Dispatcher stand-in for handler unit tests -----------------------------


class FakeDispatcher:
    """Records the jobs a handler enqueues, in order — no queue, no next_available
    stamping, no routing (that's the real Dispatcher's job, tested separately).
    Handler tests construct FakeDispatcher() and inspect `.enqueued`."""

    def __init__(self):
        self.enqueued = []

    async def enqueue(self, job):
        self.enqueued.append(job)
