"""Live acceptance tests against the deployed static actor at crawler.pub.

These hit the real site over the network, so they verify the one thing the
in-process ActorServer suite cannot: that cPanel serves each document with the
correct content type (the reason this project left GitHub Pages).

Marked `live` and deselected by default (the site has to be deployed first).
Run them with:

    uv run pytest -m live

Note: a static webfinger file is returned verbatim regardless of the query
string, so the 400 (missing resource) / 404 (unknown account) cases the dynamic
server enforced are intentionally absent here — the file always answers 200.
"""

import httpx
import pytest

from support import (
    AS2_CONTEXT,
    AS2_TYPE,
    COLLECTIONS,
    FEP_5711_CONTEXT,
    JRD_TYPE,
    LD_JSON_TYPE,
    SECURITY_CONTEXT,
    WEBFINGER_CONTEXT,
    as_list,
    media_type,
)

pytestmark = pytest.mark.live

BASE = "https://crawler.pub"
ACCT = "acct:bot@crawler.pub"
ACTOR_URL = f"{BASE}/actor"


def collection_url(prop):
    return f"{BASE}/{prop}"


@pytest.fixture(scope="module")
def client():
    # A Mastodon-like UA so we exercise the same path a real fetch would, and
    # follow redirects in case cPanel canonicalises http->https or hosts.
    with httpx.Client(
        follow_redirects=True,
        timeout=10.0,
        headers={"user-agent": "pub-crawler-tests (Mastodon-compatible)"},
    ) as c:
        yield c


@pytest.fixture(scope="module")
def actor(client):
    r = client.get(ACTOR_URL, headers={"accept": AS2_TYPE})
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Webfinger
# ---------------------------------------------------------------------------


def test_webfinger_content_type_and_subject(client):
    r = client.get(f"{BASE}/.well-known/webfinger", params={"resource": ACCT})
    assert r.status_code == 200
    assert media_type(r) == JRD_TYPE
    assert r.json()["subject"] == ACCT


def test_webfinger_advertises_activitypub_and_ld_json(client):
    r = client.get(f"{BASE}/.well-known/webfinger", params={"resource": ACCT})
    self_links = [link for link in r.json()["links"] if link.get("rel") == "self"]
    by_type = {link.get("type"): link for link in self_links}

    assert AS2_TYPE in by_type
    assert by_type[AS2_TYPE]["href"] == ACTOR_URL

    ld_types = [t for t in by_type if t and t.startswith(LD_JSON_TYPE)]
    assert ld_types, "expected an application/ld+json self link"
    ld_type = ld_types[0]
    assert "profile=" in ld_type and AS2_CONTEXT in ld_type
    assert by_type[ld_type]["href"] == ACTOR_URL


def test_webfinger_resolves_actor_id_as_resource(client):
    by_acct = client.get(f"{BASE}/.well-known/webfinger", params={"resource": ACCT})
    by_id = client.get(f"{BASE}/.well-known/webfinger", params={"resource": ACTOR_URL})
    assert by_id.status_code == 200
    assert by_id.json()["links"] == by_acct.json()["links"]


# ---------------------------------------------------------------------------
# Actor
# ---------------------------------------------------------------------------


def test_actor_content_type(client):
    r = client.get(ACTOR_URL, headers={"accept": AS2_TYPE})
    assert r.status_code == 200
    assert media_type(r) == AS2_TYPE


def test_actor_core_properties(actor):
    assert actor["id"] == ACTOR_URL
    assert actor["type"] == "Application"
    assert actor["preferredUsername"] == "bot"
    assert actor["webfinger"] == ACCT
    for prop in COLLECTIONS:
        assert actor[prop] == collection_url(prop)


def test_actor_contexts(actor):
    context = as_list(actor["@context"])
    assert AS2_CONTEXT in context
    assert SECURITY_CONTEXT in context
    assert WEBFINGER_CONTEXT in context


def test_actor_public_key(actor):
    key = actor["publicKey"]
    assert key["id"] == f"{ACTOR_URL}#main-key"
    assert key["owner"] == ACTOR_URL
    pem = key["publicKeyPem"]
    assert pem.startswith("-----BEGIN PUBLIC KEY-----")
    assert "-----END PUBLIC KEY-----" in pem


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prop,inverse", COLLECTIONS.items())
def test_collection_document(client, prop, inverse):
    url = collection_url(prop)
    r = client.get(url, headers={"accept": AS2_TYPE})
    assert r.status_code == 200
    assert media_type(r) == AS2_TYPE

    doc = r.json()
    assert doc["id"] == url
    assert doc["type"] == "OrderedCollection"
    assert doc["totalItems"] == 0
    assert doc["attributedTo"] == ACTOR_URL

    assert doc[inverse] == ACTOR_URL
    present = [v for v in COLLECTIONS.values() if v in doc]
    assert present == [inverse]

    context = as_list(doc["@context"])
    assert AS2_CONTEXT in context
    assert FEP_5711_CONTEXT in context


# ---------------------------------------------------------------------------
# Link integrity
# ---------------------------------------------------------------------------


def test_every_actor_link_resolves(client, actor):
    urls = [ACTOR_URL] + [actor[prop] for prop in COLLECTIONS]
    for url in urls:
        r = client.get(url, headers={"accept": AS2_TYPE})
        assert r.status_code == 200, f"{url} did not resolve"
        assert media_type(r) == AS2_TYPE, f"{url} wrong content type"
