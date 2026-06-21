"""Sign-then-verify tests for signature.signature_header (draft-cavage-12).

No network and no remote server: the test calls signature_header, then
independently reconstructs the canonical signing string and verifies the
returned signature against the public key derived from the same private key.
It passes only if the function built exactly the draft-cavage-12 string a
remote (e.g. Mastodon) would reconstruct — so a local pass implies remote
acceptance of the construction.

signature_header now takes a pre-loaded private key object (not a PEM string),
so the `private_key` fixture loads it once from the keypair's pem.
"""

import base64

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization

from pub_crawler.signature import signature_header
from support import canonical_signing_string, parse_signature, verify_signature

KEY_ID = "https://crawler.pub/actor#main-key"
URL = "https://remote.example/users/alice"
DATE = "Wed, 03 Jun 2026 01:00:00 GMT"


@pytest.fixture
def private_key(keypair):
    """The loaded private key object signature_header now expects (parsed once
    from the keypair's pem, mirroring how the client will hold it)."""
    pem, _ = keypair
    return serialization.load_pem_private_key(pem.encode(), password=None)


# ---------------------------------------------------------------------------
# Core: the signature verifies against the canonical string
# ---------------------------------------------------------------------------


def test_signature_verifies_against_canonical_string(keypair, private_key):
    _, public_key = keypair
    headers = {"Host": "remote.example", "Date": DATE}

    value = signature_header(URL, "GET", headers, KEY_ID, private_key)
    parsed = parse_signature(value)

    # If this doesn't raise, the function built the exact canonical string.
    verify_signature(
        public_key, parsed["signature"], canonical_signing_string("GET", URL, headers)
    )


def test_signature_header_fields(private_key):
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse_signature(signature_header(URL, "GET", headers, KEY_ID, private_key))

    assert parsed["keyId"] == KEY_ID
    assert parsed["algorithm"] == "rsa-sha256"
    assert parsed["headers"] == "(request-target) host date"


def test_signature_is_rsa_2048(private_key):
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse_signature(signature_header(URL, "GET", headers, KEY_ID, private_key))
    raw = base64.b64decode(parsed["signature"])

    assert len(raw) == 256  # 2048-bit RSA signature


# ---------------------------------------------------------------------------
# (request-target): method lowercased, path + query both signed
# ---------------------------------------------------------------------------


def test_request_target_includes_query(keypair, private_key):
    _, public_key = keypair
    url = "https://remote.example/users/alice/outbox?page=true"
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse_signature(signature_header(url, "GET", headers, KEY_ID, private_key))

    # Verifies with the query present...
    verify_signature(
        public_key, parsed["signature"], canonical_signing_string("GET", url, headers)
    )
    # ...and genuinely fails if the query is dropped, proving it was signed.
    without_query = canonical_signing_string(
        "GET", "https://remote.example/users/alice/outbox", headers
    )
    with pytest.raises(InvalidSignature):
        verify_signature(public_key, parsed["signature"], without_query)


def test_bare_domain_request_target_is_slash(keypair, private_key):
    _, public_key = keypair
    url = "https://example.com"  # no path — the wire request-target is "/"
    headers = {"Host": "example.com", "Date": DATE}

    parsed = parse_signature(signature_header(url, "GET", headers, KEY_ID, private_key))

    # Verifies against a "/" target (what an HTTP client actually sends)...
    verify_signature(
        public_key, parsed["signature"], canonical_signing_string("GET", url, headers)
    )
    # ...and not against an empty target.
    empty_target = f"(request-target): get \nhost: example.com\ndate: {DATE}"
    with pytest.raises(InvalidSignature):
        verify_signature(public_key, parsed["signature"], empty_target)


def test_method_is_lowercased_in_request_target(keypair, private_key):
    _, public_key = keypair
    url = "https://remote.example/inbox"
    headers = {
        "Host": "remote.example",
        "Date": DATE,
        "Digest": "SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
        "Content-Type": "application/activity+json",
    }

    parsed = parse_signature(signature_header(url, "POST", headers, KEY_ID, private_key))

    assert parsed["headers"] == "(request-target) host date digest content-type"
    verify_signature(
        public_key, parsed["signature"], canonical_signing_string("POST", url, headers)
    )


# ---------------------------------------------------------------------------
# Header list: dict order preserved, names lowercased
# ---------------------------------------------------------------------------


def test_header_order_preserved_and_lowercased(keypair, private_key):
    _, public_key = keypair
    # Deliberately not in canonical order, mixed case.
    headers = {"Date": DATE, "Host": "remote.example", "User-Agent": "pub-crawler/0.1"}

    parsed = parse_signature(signature_header(URL, "GET", headers, KEY_ID, private_key))

    assert parsed["headers"] == "(request-target) date host user-agent"
    verify_signature(
        public_key, parsed["signature"], canonical_signing_string("GET", URL, headers)
    )
