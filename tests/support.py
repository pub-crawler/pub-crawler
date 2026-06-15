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


# --- DatabaseGraph stand-in for handler unit tests ----------------------------


class FakeGraph:

    def __init__(self):
        self._nodes = dict()
        self._edges = dict()
        self._ids = dict()
        self._counter = 0

    async def ensure_node(self, label):
        if not label in self._nodes:
            self._nodes[label] = dict()
            self._ids[label] = self._next_counter()

    async def ensure_edge(self, from_label, to_label):
        if not from_label in self._edges:
            self._edges[from_label] = dict()
        if not to_label in self._edges[from_label]:
            self._edges[from_label][to_label] = dict()

    async def has_node(self, label):
        return label in self._nodes

    async def has_edge(self, from_label, to_label):
        return from_label in self._edges and to_label in self._edges[from_label]

    async def delete_node(self, label):
        if label in self._nodes:
            del self._nodes[label]

    async def delete_edge(self, from_label, to_label):
        if from_label in self._edges and to_label in self._edges[from_label]:
            del self._edges[from_label][to_label]

    async def set_node_property(self, label, name, value):
        self._nodes[label][name] = value

    async def set_edge_property(self, from_label, to_label, name, value):
        self._edges[from_label][to_label][name] = value

    async def get_node_property(self, label, name):
        if name in self._nodes[label]:
            return self._nodes[label][name]
        else:
            return None

    async def get_edge_property(self, from_label, to_label, name):
        return self._edges[from_label][to_label][name]

    async def get_node_properties(self, label):
        return self._nodes[label]

    async def get_edge_properties(self, from_label, to_label):
        return self._edges[from_label][to_label]

    # --- bulk variants (same semantics as the singular methods, in a loop) ---

    async def ensure_nodes(self, labels):
        for label in labels:
            await self.ensure_node(label)

    async def ensure_from_edges(self, from_label, to_labels):
        for to_label in to_labels:
            await self.ensure_edge(from_label, to_label)

    async def ensure_to_edges(self, from_labels, to_label):
        for from_label in from_labels:
            await self.ensure_edge(from_label, to_label)

    async def set_nodes_property(self, labels, name, value):
        for label in labels:
            await self.set_node_property(label, name, value)

    async def set_node_properties(self, label, properties):
        for name, value in properties.items():
            await self.set_node_property(label, name, value)

    async def set_edge_properties(self, from_label, to_label, properties):
        for name, value in properties.items():
            await self.set_edge_property(from_label, to_label, name, value)

    async def set_from_edges_property(self, from_label, to_labels, name, value):
        for to_label in to_labels:
            await self.set_edge_property(from_label, to_label, name, value)

    async def set_to_edges_property(self, from_labels, to_label, name, value):
        for from_label in from_labels:
            await self.set_edge_property(from_label, to_label, name, value)

    async def get_nodes_property(self, labels, name):
        # keyed by label; absent (missing node or missing property) omitted
        result = {}
        for label in labels:
            if label in self._nodes and name in self._nodes[label]:
                result[label] = self._nodes[label][name]
        return result

    async def get_from_edges_property(self, from_label, to_labels, name):
        # keyed by to_label (the varying endpoint); absent omitted
        result = {}
        for to_label in to_labels:
            if self._has_edge_property(from_label, to_label, name):
                result[to_label] = self._edges[from_label][to_label][name]
        return result

    async def get_to_edges_property(self, from_labels, to_label, name):
        # keyed by from_label (the varying endpoint); absent omitted
        result = {}
        for from_label in from_labels:
            if self._has_edge_property(from_label, to_label, name):
                result[from_label] = self._edges[from_label][to_label][name]
        return result

    def _has_edge_property(self, from_label, to_label, name):
        return (
            from_label in self._edges
            and to_label in self._edges[from_label]
            and name in self._edges[from_label][to_label]
        )

    async def all_nodes(self):
        for label, props in self._nodes.items():
            id = self._ids[label]
            yield id, label, props

    async def all_edges(self):
        for from_label in self._edges:
            for to_label, props in self._edges[from_label].items():
                from_node = self._ids[from_label]
                to_node = self._ids[to_label]
                yield from_node, to_node, props

    def _next_counter(self):
        self._counter += 1
        return self._counter
