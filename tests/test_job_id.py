"""Tests for job_id() -- the stable identity string used to de-duplicate jobs.

Contract (confirmed): job_id(job) is "{job_type}:{the type's url field}":
  - actor       -> "actor:{actor_id}"
  - page        -> "page:{page_id}"
  - collection  -> "collection:{collection_id}"
  - webfinger   -> "webfinger:{webfinger}"   (the one "mostly" exception -- a
                   webfinger value is an acct: handle, not really a URL)

The identity is the RESOURCE only. Fields that describe how/when we reached a
job -- depth on every type, plus direction/owner_id on page & collection -- are
NOT part of the id: the same actor reached at depth 1 and again at depth 2 is
ONE job. That invariance is the whole point of de-duplication, so it is pinned
explicitly below.

Black-box: written from the contract above, not from job_id()'s source.
"""

from pub_crawler.job_id import job_id

ACTOR = "https://x.example/users/a"
PAGE = "https://x.example/users/a/followers?page=1"
COLLECTION = "https://x.example/users/a/followers"
WF = "acct:a@x.example"
OWNER = "https://x.example/users/a"


def actor_job(actor_id=ACTOR, depth=1):
    return {"job_type": "actor", "actor_id": actor_id, "depth": depth}


def page_job(page_id=PAGE, *, owner_id=OWNER, direction="followers", depth=1):
    return {
        "job_type": "page",
        "page_id": page_id,
        "owner_id": owner_id,
        "direction": direction,
        "depth": depth,
    }


def collection_job(collection_id=COLLECTION, *, owner_id=OWNER, direction="followers", depth=1):
    return {
        "job_type": "collection",
        "collection_id": collection_id,
        "owner_id": owner_id,
        "direction": direction,
        "depth": depth,
    }


def webfinger_job(wf=WF):
    return {"job_type": "webfinger", "webfinger": wf}


# ---------------------------------------------------------------------------
# Format per type: "{job_type}:{url field}"
# ---------------------------------------------------------------------------


def test_actor_id_is_type_and_actor_id():
    assert job_id(actor_job()) == f"actor:{ACTOR}"


def test_page_id_is_type_and_page_id():
    assert job_id(page_job()) == f"page:{PAGE}"


def test_collection_id_is_type_and_collection_id():
    assert job_id(collection_job()) == f"collection:{COLLECTION}"


def test_webfinger_id_is_type_and_webfinger_handle():
    # The "mostly" exception: the url part is an acct: handle, not an https URL.
    assert job_id(webfinger_job()) == f"webfinger:{WF}"


# ---------------------------------------------------------------------------
# Deterministic: equal content -> equal id
# ---------------------------------------------------------------------------


def test_equal_jobs_get_equal_ids():
    assert job_id(actor_job()) == job_id(actor_job())


# ---------------------------------------------------------------------------
# Identity excludes the "how we got here" fields -- the de-dup property
# ---------------------------------------------------------------------------


def test_actor_id_ignores_depth():
    # Same actor reached at different depths is ONE job.
    assert job_id(actor_job(depth=1)) == job_id(actor_job(depth=2))


def test_page_id_ignores_depth_direction_and_owner():
    # Only page_id identifies a page; depth/direction/owner_id do not.
    a = page_job(depth=1, direction="followers", owner_id="https://x.example/users/a")
    b = page_job(depth=5, direction="following", owner_id="https://y.example/users/b")
    assert job_id(a) == job_id(b)


def test_collection_id_ignores_depth_direction_and_owner():
    a = collection_job(depth=1, direction="followers", owner_id="https://x.example/users/a")
    b = collection_job(depth=9, direction="following", owner_id="https://y.example/users/b")
    assert job_id(a) == job_id(b)


# ---------------------------------------------------------------------------
# Distinct resources -> distinct ids
# ---------------------------------------------------------------------------


def test_different_actors_get_different_ids():
    other = "https://x.example/users/b"
    assert job_id(actor_job(actor_id=ACTOR)) != job_id(actor_job(actor_id=other))


def test_the_type_prefix_discriminates_same_url_across_types():
    # The same URL appearing as both an actor_id and a page_id is NOT the same
    # job -- the type prefix keeps them distinct.
    url = "https://x.example/users/a"
    assert job_id(actor_job(actor_id=url)) != job_id(page_job(page_id=url))


# ---------------------------------------------------------------------------
# Malformed jobs: job_id() returns None -- NOT a bogus string like "actor:None"
# (which would collapse every unidentifiable job onto one id in the seen set),
# and NOT an exception. A None id lets callers skip a job they can't identify.
# ---------------------------------------------------------------------------


def test_non_dict_argument_is_none():
    assert job_id(5) is None


def test_missing_job_type_is_none():
    assert job_id({"actor_id": ACTOR}) is None


def test_unknown_job_type_is_none():
    assert job_id({"job_type": "incorrect", "actor_id": ACTOR}) is None


def test_actor_without_actor_id_is_none():
    assert job_id({"job_type": "actor", "depth": 1}) is None


def test_page_without_page_id_is_none():
    assert job_id({"job_type": "page", "owner_id": OWNER, "direction": "followers"}) is None


def test_collection_without_collection_id_is_none():
    assert job_id({"job_type": "collection", "owner_id": OWNER, "direction": "followers"}) is None


def test_webfinger_without_handle_is_none():
    assert job_id({"job_type": "webfinger"}) is None
