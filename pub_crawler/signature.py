from urllib.parse import urlsplit
import base64
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding


def signature_header(url, method, headers, key_id, pem):
    sstr = signing_string(url, method, headers)
    signature = sign(sstr, pem)
    hstr = header_string(headers)
    return format_header(
        {
            "keyId": key_id,
            "algorithm": "rsa-sha256",
            "headers": hstr,
            "signature": signature,
        }
    )


def header_string(headers):
    items = ["(request-target)"]
    for name in headers:
        items.append(name.lower())
    return " ".join(items)


def signing_string(url, method, headers):
    parts = urlsplit(url)
    if parts.query:
        pathPart = f"{parts.path}?{parts.query}"
    else:
        pathPart = parts.path or "/"

    lines = []
    lines.append(f"(request-target): {method.lower()} {pathPart}")

    for name, value in headers.items():
        lines.append(f"{name.lower()}: {value}")

    return "\n".join(lines)


def sign(sstr, pem):

    key = serialization.load_pem_private_key(pem.encode(), password=None)
    sig = key.sign(sstr.encode(), padding.PKCS1v15(), hashes.SHA256())
    return base64.b64encode(sig).decode()


def format_header(fields):
    parts = []
    for name, value in fields.items():
        parts.append(f'{name}="{escape(value)}"')
    return ",".join(parts)


def escape(value):
    return value.replace('"', '\\"')
