"""Shared constants and helpers for the live test suite."""

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
