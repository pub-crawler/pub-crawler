"""Tests for bin/survey_hosts.py — orchestration of the host survey.

Two separately-testable async functions (importable, bin/ is on pythonpath);
main() composes them and owns the --no-seed decision. Since the HostSurvey
split, hosts live in their own container: the graph supplies node labels,
the survey container holds hosts and their properties.

  seed_hosts_from_nodes(graph, survey) -> None
    Stream graph.all_nodes(), take the hostname of each label URL
    (urlparse().hostname — lowercased, port stripped), SKIP labels that
    yield no hostname, dedupe, and survey.ensure_hosts() the rest (batched;
    conflict-ignoring, so hosts that already exist — with survey data —
    are untouched).

  survey_hosts(survey, surveyor, *, max_age, max_workers=50, limit=None) -> int
    Scan: a host is due when its last_fetch_date property is absent, or is
    an ISO-8601 timestamp older than now - max_age (a timedelta; the CLI
    converts --max-age). Survey: at most max_workers
    surveyor.survey(hostname) calls in flight; each returned dict is saved
    with survey.set_host_properties(hostname, result) as it completes —
    per-host saves are the resume granularity. When limit is not None, at
    most that many hosts are surveyed (smoke runs). One host's failure
    (surveying or saving) must not abort the rest. Returns the number of
    hosts surveyed.

Uses FakeGraph + FakeHostSurvey and a fake surveyor; the real
HostSurveyor/NodeinfoClient and argparse plumbing are not exercised here.

Assumed contract (adjust the tests if the shape differs).
"""

import asyncio
from datetime import datetime, timedelta, timezone

from support import FakeGraph, FakeHostSurvey
from survey_hosts import seed_hosts_from_nodes, survey_hosts

WEEK = timedelta(weeks=1)
NOW = datetime.now(timezone.utc)
STALE = (NOW - timedelta(days=8)).isoformat()
FRESH = (NOW - timedelta(hours=1)).isoformat()


class FakeSurveyor:
    """Records surveyed hostnames and peak concurrency; returns a canned
    properties dict (always including last_fetch_date, like the real one)."""

    def __init__(self, result=None, delay=0):
        self.surveyed = []
        self.active = 0
        self.peak = 0
        self.delay = delay
        self.result = result or {
            "last_fetch_date": NOW.isoformat(),
            "software_name": "mastodon",
        }

    async def survey(self, hostname):
        self.active += 1
        self.peak = max(self.peak, self.active)
        if self.delay:
            await asyncio.sleep(self.delay)
        self.active -= 1
        self.surveyed.append(hostname)
        return dict(self.result)


async def survey_with_hosts(**last_fetch_dates):
    """A FakeHostSurvey holding the given hosts; a None value means the host
    has never been surveyed (no last_fetch_date property)."""
    survey = FakeHostSurvey()
    for hostname, lfd in last_fetch_dates.items():
        await survey.ensure_host(hostname)
        if lfd is not None:
            await survey.set_host_property(hostname, "last_fetch_date", lfd)
    return survey


# ---------------------------------------------------------------------------
# Seeding: node labels -> hostnames (its own function, composed by main)
# ---------------------------------------------------------------------------


async def test_seeds_hostnames_from_node_labels():
    graph = FakeGraph()
    await graph.ensure_node("https://A.Example/users/alice")  # mixed case
    await graph.ensure_node("https://a.example/users/bob")  # same host
    await graph.ensure_node("https://b.example:8443/actor")  # port stripped
    await graph.ensure_node("urn:uuid:1234")  # no hostname -> skipped
    survey = FakeHostSurvey()

    await seed_hosts_from_nodes(graph, survey)

    hostnames = {h async for _, h, _ in survey.all_hosts()}
    assert hostnames == {"a.example", "b.example"}


async def test_seeding_is_idempotent_with_existing_hosts():
    # Hosts already known (with survey data) survive re-seeding untouched.
    survey = await survey_with_hosts(**{"a.example": FRESH})
    graph = FakeGraph()
    await graph.ensure_node("https://a.example/users/alice")

    await seed_hosts_from_nodes(graph, survey)

    assert await survey.get_host_property("a.example", "last_fetch_date") == FRESH


async def test_seeded_hosts_are_due_for_survey():
    # The composition main() relies on: freshly seeded hosts carry no
    # last_fetch_date, so a following survey_hosts picks them all up.
    graph = FakeGraph()
    await graph.ensure_node("https://a.example/users/alice")
    survey = FakeHostSurvey()
    surveyor = FakeSurveyor()

    await seed_hosts_from_nodes(graph, survey)
    count = await survey_hosts(survey, surveyor, max_age=WEEK)

    assert surveyor.surveyed == ["a.example"]
    assert count == 1


# ---------------------------------------------------------------------------
# Due selection: absent or stale last_fetch_date
# ---------------------------------------------------------------------------


async def test_surveys_never_surveyed_and_stale_hosts_only():
    survey = await survey_with_hosts(
        **{"never.example": None, "stale.example": STALE, "fresh.example": FRESH}
    )
    surveyor = FakeSurveyor()

    count = await survey_hosts(survey, surveyor, max_age=WEEK)

    assert sorted(surveyor.surveyed) == ["never.example", "stale.example"]
    assert count == 2


# ---------------------------------------------------------------------------
# Saving: each survey result lands via set_host_properties
# ---------------------------------------------------------------------------


async def test_saves_the_survey_result_as_host_properties():
    survey = await survey_with_hosts(**{"a.example": None})
    surveyor = FakeSurveyor(
        result={"last_fetch_date": NOW.isoformat(), "failure": "dns_error"}
    )

    await survey_hosts(survey, surveyor, max_age=WEEK)

    props = await survey.get_host_properties("a.example")
    assert props["failure"] == "dns_error"
    assert props["last_fetch_date"] == NOW.isoformat()


# ---------------------------------------------------------------------------
# limit: bounds total surveys (smoke runs)
# ---------------------------------------------------------------------------


async def test_limit_bounds_the_number_of_surveys():
    survey = await survey_with_hosts(
        **{f"h{i}.example": None for i in range(5)},
    )
    surveyor = FakeSurveyor()

    count = await survey_hosts(survey, surveyor, max_age=WEEK, limit=2)

    assert count == 2
    assert len(surveyor.surveyed) == 2


# ---------------------------------------------------------------------------
# Robustness: one host's failing save doesn't abort the run
# ---------------------------------------------------------------------------


class SaveExplodesFor(FakeHostSurvey):
    def __init__(self, bad_hostname):
        super().__init__()
        self.bad_hostname = bad_hostname

    async def set_host_properties(self, hostname, properties):
        if hostname == self.bad_hostname:
            raise RuntimeError("db hiccup")
        await super().set_host_properties(hostname, properties)


async def test_one_failing_save_does_not_abort_the_run():
    survey = SaveExplodesFor("bad.example")
    for hostname in ("a.example", "bad.example", "z.example"):
        await survey.ensure_host(hostname)
    surveyor = FakeSurveyor()

    await survey_hosts(survey, surveyor, max_age=WEEK)

    # The other hosts still got surveyed and saved.
    assert "a.example" in surveyor.surveyed
    assert "z.example" in surveyor.surveyed
    assert "last_fetch_date" in await survey.get_host_properties("a.example")
    assert "last_fetch_date" in await survey.get_host_properties("z.example")


# ---------------------------------------------------------------------------
# Concurrency: at most max_workers surveys in flight
# ---------------------------------------------------------------------------


async def test_concurrency_is_bounded_by_max_workers():
    survey = await survey_with_hosts(**{f"h{i}.example": None for i in range(6)})
    surveyor = FakeSurveyor(delay=0.005)

    await survey_hosts(survey, surveyor, max_age=WEEK, max_workers=2)

    assert len(surveyor.surveyed) == 6
    assert surveyor.peak <= 2
