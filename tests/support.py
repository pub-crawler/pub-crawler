"""Shared constants and helpers for the ActorServer test suite."""

from urllib.parse import urlsplit

# The origin the server is configured with in tests. Because the server
# generates all of its URLs from the origin passed at construction (never from
# the inbound request), this value should appear in every id/link it emits,
# regardless of what host a request actually arrives on.
ORIGIN = "https://test.example"

# The actor's preferredUsername / webfinger account name.
USERNAME = "bot"

# A fixed RSA public key (SPKI PEM) injected into the server in tests, so the
# publicKeyPem the actor serves can be asserted to be exactly what was passed in
# — pinning that the server publishes the crawler's key rather than inventing one.
PUBLIC_KEY_PEM = """\
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmUaYoFrMpZx+9p8yqS5N
X/sPZTKIU58C9wmfOH4RDARMNRtffhsfcIMaz2uYqpWwoe2KEVmuQ77CAU+WMjBi
MKp4AM9gA/gsAbpu5KhidsYP+NqsE5HzpOkNUwuMYB4oP/LsvIsN33J2X3giVeSe
p8yMCwEbB83rVj//yUPOOA0p2Le4S9Wj5OnMllaL3y6Wed2zwlciVj8RDdUvwlMU
FhXKxlZSEmP7Xjr+yvb3THAvHPrGIvhK1nVSh7zUuBm3Zw7byZRm7gKJKG1cnF0u
LuV5gxMckcMZ2Y16cadKswvtXylISPMlorB/NHEMViQlaOcSQyi26U93I5Ly4UDx
1wIDAQAB
-----END PUBLIC KEY-----
"""

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


def host_of(url):
    return urlsplit(url).netloc


def media_type(response):
    """The bare media type of a response, without parameters or charset."""
    return response.headers["content-type"].split(";")[0].strip().lower()


def as_list(value):
    """Normalise a JSON-LD value that may be a scalar or a list into a list."""
    if isinstance(value, list):
        return value
    return [value]


def webfinger_resource(origin=ORIGIN, username=USERNAME):
    return f"acct:{username}@{host_of(origin)}"


async def discover_actor(client):
    """Mirror Mastodon's discovery: webfinger -> actor document.

    Returns (actor_url, actor_document). Keeps the suite path-agnostic: nothing
    here assumes a particular actor path, only the well-known webfinger route.
    """
    r = await client.get(
        "/.well-known/webfinger",
        params={"resource": webfinger_resource()},
    )
    r.raise_for_status()
    jrd = r.json()
    self_link = next(
        link
        for link in jrd["links"]
        if link.get("rel") == "self" and link.get("type") == AS2_TYPE
    )
    actor_url = self_link["href"]

    actor = await client.get(actor_url, headers={"accept": AS2_TYPE})
    actor.raise_for_status()
    return actor_url, actor.json()
