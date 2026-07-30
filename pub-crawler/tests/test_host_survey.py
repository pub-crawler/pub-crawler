"""Contract tests for the HostSurvey interface — every implementation must satisfy.

Hosts are their own container, not part of the graph: a `host` table
(id, hostname UNIQUE, created_at) and a `host_property` EAV table (JSONB
values), wrapped by DatabaseHostSurvey (pub_crawler/database_host_survey.py,
sharing the asyncpg pool with DatabaseGraph) and mirrored in-memory by
FakeHostSurvey (tests/support.py). The API:

  await ensure_host(hostname) / ensure_hosts(hostnames)
  await has_host(hostname) -> bool / delete_host(hostname)
  await set_host_property(hostname, name, value)
  await set_hosts_property(hostnames, name, value)     # same value on many
  await set_host_properties(hostname, properties)      # many values on one
  await delete_host_properties(hostname, names)        # drop named props
  await get_host_property(hostname, name)  -> value (None if absent)
  await get_hosts_property(hostnames, name) -> {hostname: value}, absent omitted
  await get_host_properties(hostname)      -> {name: value}
  async for host_id, hostname, props in all_hosts()    # int id, streaming

Like the graph contract in test_graph.py, the same assertions run against
each backend through the parametrized `survey` fixture: FakeHostSurvey
always, and DatabaseHostSurvey against a real Postgres when
TEST_DATABASE_URL is set (the `db`-marked params, deselected by default).

Assumptions flagged for confirmation:
  - Hostnames arrive already normalized (lowercase, A-label) — the survey
    driver's job. The container stores what it's given; no normalization
    pinned.
  - Re-ensuring existing hosts must not consume identity values (anti-join
    before insert, ON CONFLICT only as the race net) — ids stay (mostly)
    consecutive.
  - get_hosts_property omits hostnames lacking the property — that absence
    is the survey's "never surveyed" gate.
  - all_hosts() streams (server-side cursor, own transaction), like
    all_nodes().
  - delete_host cascades to the host's properties (FK ON DELETE CASCADE).
"""

import asyncio

import pytest

from support import FakeHostSurvey

H1 = "a.example"
H2 = "b.example"
H3 = "c.example"


@pytest.fixture(params=["fake", pytest.param("db", marks=pytest.mark.db)])
async def survey(request):
    """The contract subject, one per backend — same shape as test_graph.py's."""
    if request.param == "fake":
        yield FakeHostSurvey()
        return

    dsn = request.getfixturevalue("pg_dsn")  # skips if unset / asyncpg missing
    import asyncpg

    try:
        from pub_crawler.database import database_setup
        from pub_crawler.database_host_survey import DatabaseHostSurvey
    except ImportError as exc:
        pytest.skip(f"DatabaseHostSurvey not implemented yet ({exc})")

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    try:
        async with pool.acquire() as conn:
            await database_setup(conn)  # applies the host migrations via the ledger
            await conn.execute("TRUNCATE host, host_property RESTART IDENTITY CASCADE")
        yield DatabaseHostSurvey(pool)
    finally:
        await pool.close()


async def hostnames_in(survey):
    """The set of hostnames the container currently holds, via all_hosts (so
    the ensure tests also pin iteration, independently of has_host)."""
    return {hostname async for _, hostname, _ in survey.all_hosts()}


# ---------------------------------------------------------------------------
# ensure: single, bulk, idempotent
# ---------------------------------------------------------------------------


async def test_ensure_host_then_all_hosts_contains_it(survey):
    assert await hostnames_in(survey) == set()
    await survey.ensure_host(H1)
    assert await hostnames_in(survey) == {H1}


async def test_ensure_host_is_idempotent(survey):
    await survey.ensure_host(H1)
    await survey.ensure_host(H1)  # no error, still one host

    assert [h async for _, h, _ in survey.all_hosts()] == [H1]


async def test_ensure_hosts_creates_all_and_is_idempotent(survey):
    await survey.ensure_hosts([])  # empty is a no-op, not an error
    await survey.ensure_hosts([H1, H2, H3])
    await survey.ensure_hosts([H1, H2, H3])  # idempotent

    assert await hostnames_in(survey) == {H1, H2, H3}


async def test_re_ensuring_does_not_burn_ids(survey):
    # Host ids should stay (mostly) consecutive: re-ensuring an existing host
    # must not consume identity values (the insert filters existing rows
    # before the default is evaluated, rather than relying on ON CONFLICT
    # alone). Concurrent-race gaps are tolerated; systematic re-ensure burn
    # is not.
    await survey.ensure_host(H1)
    await survey.ensure_host(H1)  # re-ensure, single
    await survey.ensure_hosts([H1])  # re-ensure, bulk
    await survey.ensure_host(H2)

    ids = {h: host_id async for host_id, h, _ in survey.all_hosts()}

    assert ids[H2] == ids[H1] + 1


async def test_ensure_hosts_mixed_batch_does_not_burn_ids(survey):
    # Bulk ensure where one member already exists: only the genuinely new
    # hostnames may consume ids (mirrors the node-family contract).
    await survey.ensure_host(H1)
    await survey.ensure_hosts([H1, H2, H3])  # H1 exists; H2 and H3 are new

    ids = {h: host_id async for host_id, h, _ in survey.all_hosts()}

    assert sorted((ids[H2], ids[H3])) == [ids[H1] + 1, ids[H1] + 2]


# ---------------------------------------------------------------------------
# has / delete
# ---------------------------------------------------------------------------


async def test_has_host_after_ensure(survey):
    assert not await survey.has_host(H1)
    await survey.ensure_host(H1)
    assert await survey.has_host(H1)


async def test_delete_host(survey):
    await survey.ensure_host(H1)
    await survey.delete_host(H1)
    assert not await survey.has_host(H1)


async def test_delete_host_after_its_id_was_looked_up(survey):
    # Mirrors test_graph.py's cache pin: if a backend caches hostname->id,
    # looking the host up first seeds that cache and delete_host must
    # invalidate it, or has_host keeps reporting a stale, deleted id.
    await survey.ensure_host(H1)
    assert await survey.has_host(H1)  # seeds any hostname->id cache
    await survey.delete_host(H1)
    assert not await survey.has_host(H1)  # must not return a stale cached id


# ---------------------------------------------------------------------------
# host properties (jsonb: types survive the round-trip)
# ---------------------------------------------------------------------------


async def test_set_and_get_host_property_keeps_type(survey):
    await survey.ensure_host(H1)
    await survey.set_host_property(H1, "users_total", 42)
    await survey.set_host_property(H1, "software_name", "mastodon")
    assert await survey.get_host_property(H1, "users_total") == 42  # int, not "42"
    assert await survey.get_host_property(H1, "software_name") == "mastodon"


async def test_get_host_property_absent_is_none(survey):
    await survey.ensure_host(H1)
    assert await survey.get_host_property(H1, "missing") is None


async def test_set_host_property_overwrites(survey):
    await survey.ensure_host(H1)
    await survey.set_host_property(H1, "failure", "connect_error")
    await survey.set_host_property(H1, "failure", "dns_error")
    assert await survey.get_host_property(H1, "failure") == "dns_error"


async def test_get_host_properties_returns_all(survey):
    await survey.ensure_host(H1)
    await survey.set_host_property(H1, "failure", "dns_error")
    await survey.set_host_property(H1, "users_total", 5)
    assert await survey.get_host_properties(H1) == {
        "failure": "dns_error",
        "users_total": 5,
    }


async def test_get_host_properties_of_a_bare_host_is_empty(survey):
    await survey.ensure_host(H1)
    assert await survey.get_host_properties(H1) == {}


# ---------------------------------------------------------------------------
# bulk property variants — same observable behaviour as the singular methods
# ---------------------------------------------------------------------------


async def test_set_host_properties_sets_many_on_one_host(survey):
    # The survey's save: one host's whole result dict in one call.
    await survey.ensure_host(H1)
    await survey.set_host_properties(
        H1,
        {
            "last_fetch_date": "2026-07-30T12:00:00+00:00",
            "software_name": "mastodon",
            "users_total": 890000,
        },
    )

    props = await survey.get_host_properties(H1)

    assert props["software_name"] == "mastodon"
    assert props["users_total"] == 890000  # int survives
    assert props["last_fetch_date"] == "2026-07-30T12:00:00+00:00"


async def test_set_hosts_property_sets_same_on_every_host(survey):
    await survey.ensure_hosts([H1, H2])
    await survey.set_hosts_property([H1, H2], "failure", "dns_error")
    assert await survey.get_host_property(H1, "failure") == "dns_error"
    assert await survey.get_host_property(H2, "failure") == "dns_error"


async def test_get_hosts_property_keyed_by_hostname_omits_absent(survey):
    # The never-surveyed gate: only hosts that HAVE last_fetch_date come back,
    # so the due set includes the difference.
    await survey.ensure_hosts([H1, H2, H3])
    await survey.set_host_property(H1, "last_fetch_date", "2026-07-01T00:00:00+00:00")
    await survey.set_host_property(H2, "last_fetch_date", "2026-07-22T00:00:00+00:00")
    # H3 has never been surveyed

    result = await survey.get_hosts_property([H1, H2, H3], "last_fetch_date")

    assert result == {
        H1: "2026-07-01T00:00:00+00:00",
        H2: "2026-07-22T00:00:00+00:00",
    }
    assert {H1, H2, H3} - result.keys() == {H3}  # the "never surveyed" use case


async def test_get_hosts_property_empty_input_is_empty_dict(survey):
    assert await survey.get_hosts_property([], "anything") == {}


async def test_delete_host_properties_removes_only_the_named(survey):
    # The survey's stale-property eraser: a re-survey deletes the properties
    # its new result has no value for (e.g. failure after a host recovers,
    # software_name after a host dies), leaving the rest untouched.
    await survey.ensure_host(H1)
    await survey.set_host_properties(
        H1, {"failure": "connect_error", "error_detail": "x", "users_total": 5}
    )

    await survey.delete_host_properties(H1, ["failure", "error_detail"])

    assert await survey.get_host_properties(H1) == {"users_total": 5}


async def test_delete_host_properties_tolerates_absent_names(survey):
    # Deleting names the host doesn't carry (the common case: most surveys
    # have nothing stale to erase) is a no-op, not an error; empty too.
    await survey.ensure_host(H1)
    await survey.set_host_property(H1, "users_total", 5)

    await survey.delete_host_properties(H1, ["failure", "software_name"])
    await survey.delete_host_properties(H1, [])

    assert await survey.get_host_properties(H1) == {"users_total": 5}


# ---------------------------------------------------------------------------
# iteration (the scan and the host snapshot export)
# ---------------------------------------------------------------------------


async def test_all_hosts_yields_id_hostname_and_props(survey):
    await survey.ensure_host(H1)
    await survey.set_host_property(H1, "failure", "dns_error")
    await survey.ensure_host(H2)  # no props

    by_hostname = {
        hostname: (host_id, props)
        async for host_id, hostname, props in survey.all_hosts()
    }

    assert by_hostname.keys() == {H1, H2}
    assert by_hostname[H1][1] == {"failure": "dns_error"}
    assert by_hostname[H2][1] == {}
    # ids are integers and distinct per host
    assert isinstance(by_hostname[H1][0], int)
    assert by_hostname[H1][0] != by_hostname[H2][0]


# ---------------------------------------------------------------------------
# concurrency — the survey's ~50 workers all share ONE container
# ---------------------------------------------------------------------------


async def test_concurrent_operations_share_the_connection_safely(survey):
    """Mirrors the graph-side concurrency contract: overlapping ensure/set ops
    from gathered tasks must serialize safely on the shared pool."""
    hosts = [f"h{i}.example" for i in range(20)]

    await asyncio.gather(*(survey.ensure_host(h) for h in hosts))
    await asyncio.gather(
        *(survey.set_host_property(h, "n", i) for i, h in enumerate(hosts))
    )

    for i, h in enumerate(hosts):
        assert await survey.get_host_property(h, "n") == i
