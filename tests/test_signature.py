"""Sign-then-verify tests for signature.signature_header (draft-cavage-12).

No network and no remote server: the test calls signature_header, then
independently reconstructs the canonical signing string and verifies the
returned signature against the public key derived from the same private pem.
It passes only if the function built exactly the draft-cavage-12 string a
remote (e.g. Mastodon) would reconstruct — so a local pass implies remote
acceptance of the construction.
"""

import base64
import re
from urllib.parse import urlsplit

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from signature import signature_header

KEY_ID = "https://crawler.pub/actor#main-key"
URL = "https://remote.example/users/alice"
DATE = "Wed, 03 Jun 2026 01:00:00 GMT"


@pytest.fixture(scope="module")
def keypair():
    """(private_pem_str, public_key_object) — fresh RSA-2048 for the suite."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    return pem, key.public_key()


def parse(header_value):
    """Parse a Signature header value into its keyId/algorithm/headers/signature."""
    return dict(re.findall(r'(\w+)="([^"]*)"', header_value))


def canonical_signing_string(method, url, headers):
    """The draft-cavage-12 signing string a verifier reconstructs."""
    parts = urlsplit(url)
    target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
    lines = [f"(request-target): {method.lower()} {target}"]
    for name, value in headers.items():
        lines.append(f"{name.lower()}: {value}")
    return "\n".join(lines)


def verify(public_key, signature_b64, message):
    """Raises InvalidSignature if the signature doesn't match the message."""
    public_key.verify(
        base64.b64decode(signature_b64),
        message.encode(),
        padding.PKCS1v15(),
        hashes.SHA256(),
    )


# ---------------------------------------------------------------------------
# Core: the signature verifies against the canonical string
# ---------------------------------------------------------------------------


def test_signature_verifies_against_canonical_string(keypair):
    pem, public_key = keypair
    headers = {"Host": "remote.example", "Date": DATE}

    value = signature_header(URL, "GET", headers, KEY_ID, pem)
    parsed = parse(value)

    # If this doesn't raise, the function built the exact canonical string.
    verify(public_key, parsed["signature"], canonical_signing_string("GET", URL, headers))


def test_signature_header_fields(keypair):
    pem, _ = keypair
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse(signature_header(URL, "GET", headers, KEY_ID, pem))

    assert parsed["keyId"] == KEY_ID
    assert parsed["algorithm"] == "rsa-sha256"
    assert parsed["headers"] == "(request-target) host date"


def test_signature_is_rsa_2048(keypair):
    pem, _ = keypair
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse(signature_header(URL, "GET", headers, KEY_ID, pem))
    raw = base64.b64decode(parsed["signature"])

    assert len(raw) == 256  # 2048-bit RSA signature


# ---------------------------------------------------------------------------
# (request-target): method lowercased, path + query both signed
# ---------------------------------------------------------------------------


def test_request_target_includes_query(keypair):
    pem, public_key = keypair
    url = "https://remote.example/users/alice/outbox?page=true"
    headers = {"Host": "remote.example", "Date": DATE}

    parsed = parse(signature_header(url, "GET", headers, KEY_ID, pem))

    # Verifies with the query present...
    verify(public_key, parsed["signature"], canonical_signing_string("GET", url, headers))
    # ...and genuinely fails if the query is dropped, proving it was signed.
    without_query = canonical_signing_string(
        "GET", "https://remote.example/users/alice/outbox", headers
    )
    with pytest.raises(InvalidSignature):
        verify(public_key, parsed["signature"], without_query)


def test_bare_domain_request_target_is_slash(keypair):
    pem, public_key = keypair
    url = "https://example.com"  # no path — the wire request-target is "/"
    headers = {"Host": "example.com", "Date": DATE}

    parsed = parse(signature_header(url, "GET", headers, KEY_ID, pem))

    # Verifies against a "/" target (what an HTTP client actually sends)...
    verify(public_key, parsed["signature"], canonical_signing_string("GET", url, headers))
    # ...and not against an empty target.
    empty_target = f"(request-target): get \nhost: example.com\ndate: {DATE}"
    with pytest.raises(InvalidSignature):
        verify(public_key, parsed["signature"], empty_target)


def test_method_is_lowercased_in_request_target(keypair):
    pem, public_key = keypair
    url = "https://remote.example/inbox"
    headers = {
        "Host": "remote.example",
        "Date": DATE,
        "Digest": "SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=",
        "Content-Type": "application/activity+json",
    }

    parsed = parse(signature_header(url, "POST", headers, KEY_ID, pem))

    assert parsed["headers"] == "(request-target) host date digest content-type"
    verify(public_key, parsed["signature"], canonical_signing_string("POST", url, headers))


# ---------------------------------------------------------------------------
# Header list: dict order preserved, names lowercased
# ---------------------------------------------------------------------------


def test_header_order_preserved_and_lowercased(keypair):
    pem, public_key = keypair
    # Deliberately not in canonical order, mixed case.
    headers = {"Date": DATE, "Host": "remote.example", "User-Agent": "pub-crawler/0.1"}

    parsed = parse(signature_header(URL, "GET", headers, KEY_ID, pem))

    assert parsed["headers"] == "(request-target) date host user-agent"
    verify(public_key, parsed["signature"], canonical_signing_string("GET", URL, headers))
