import logging

from pub_crawler.host_surveyor import HostSurveyor
from pub_crawler.nodeinfo_client import NodeinfoClient
from pub_crawler.database import database_setup
from pub_crawler.database_graph import DatabaseGraph
from pub_crawler.database_host_survey import DatabaseHostSurvey
from pub_crawler.fixed_window_counter import FixedWindowCounter
from pub_crawler.throttle import (
    BURST_LIMIT,
    BURST_WINDOW,
    GENERAL_LIMIT,
    GENERAL_WINDOW,
)
import asyncio
import uvloop
import asyncpg
from datetime import timedelta, datetime, timezone
from urllib.parse import urlparse

DEFAULT_MAX_AGE = "1d"
DEFAULT_MAX_WORKERS = 50
DEFAULT_LIMIT = None

SEED_HOSTS_REPORT_LIMIT = 100_000
SEED_HOSTS_BATCH_LIMIT = 1000

SCAN_HOST_REPORT_LIMIT = 10_000
SURVEY_HOST_REPORT_LIMIT = 100

async def seed_hosts_from_nodes(G, H):
    node_count = 0

    hostnames = set()
    async for _, actor_id, _ in G.all_nodes():
        node_count += 1
        hostname = urlparse(actor_id).hostname
        if hostname is not None:
            hostnames.add(hostname)
        if node_count % SEED_HOSTS_REPORT_LIMIT == 0:
            logging.info(f"{node_count} nodes, {len(hostnames)} hosts seen")

    logging.info(f"{node_count} nodes, {len(hostnames)} hosts seen")

    batch = []
    host_count = 0
    for hostname in hostnames:
        host_count += 1
        batch.append(hostname)
        if len(batch) >= SEED_HOSTS_BATCH_LIMIT:
            await H.ensure_hosts(batch)
            batch = []
            logging.info(f"{host_count} hosts ensured")

    if len(batch) > 0:
        await H.ensure_hosts(batch)
        batch = []
        logging.info(f"{host_count} hosts ensured")


async def survey_worker(queue, H, surveyor):
    while True:
        hostname = await queue.get()
        try:
            props = await surveyor.survey(hostname)
            delete_props = [k for k, v in props.items() if v is None]
            set_props = {k: v for k, v in props.items() if v is not None}
            await H.delete_host_properties(hostname, delete_props)
            await H.set_host_properties(hostname, set_props)
        except Exception as e:
            logging.warning(f"{hostname}: {e!r}")
        finally:
            queue.task_done()


async def survey_hosts(
    H, surveyor, *, max_age, max_workers=DEFAULT_MAX_WORKERS, limit=DEFAULT_LIMIT
):

    queue = asyncio.Queue(maxsize=max_workers * 2)

    workers = [
        asyncio.create_task(survey_worker(queue, H, surveyor))
        for _ in range(max_workers)
    ]

    cutoff = datetime.now(timezone.utc) - max_age
    to_survey = []
    count = 0
    async for _, hostname, props in H.all_hosts():
        count += 1
        if "last_fetch_date" not in props:
            to_survey.append(hostname)
        else:
            try:
                fetched = datetime.fromisoformat(props["last_fetch_date"])
            except (ValueError, TypeError):
                fetched = None

            if fetched is None or fetched < cutoff:
                to_survey.append(hostname)

        if limit is not None and len(to_survey) >= limit:
            break

        if count % SCAN_HOST_REPORT_LIMIT == 0:
            logging.info(f"{count} hosts seen, {len(to_survey)} to survey")

    logging.info(f"FINAL: {count} hosts seen, {len(to_survey)} to survey")

    survey_count = 0

    for hostname in to_survey:
        survey_count += 1
        await queue.put(hostname)
        if survey_count % SURVEY_HOST_REPORT_LIMIT == 0:
            logging.info(f"{survey_count} hosts surveyed")

    await queue.join()

    for w in workers:
        w.cancel()

    await asyncio.gather(*workers, return_exceptions=True)

    return survey_count


async def main(database_url, max_age, max_workers, limit, seed):
    max_conns = max(max_workers // 2, 1)
    pool = await asyncpg.create_pool(
        database_url, max_size=max_conns, min_size=min(max_conns, 10)
    )
    async with pool.acquire() as conn:
        await database_setup(conn)
    G = DatabaseGraph(pool)
    H = DatabaseHostSurvey(pool)
    burst = FixedWindowCounter(BURST_LIMIT, BURST_WINDOW)
    general = FixedWindowCounter(GENERAL_LIMIT, GENERAL_WINDOW)
    client = NodeinfoClient(general, burst, transport=None, max_workers=max_workers)
    surveyor = HostSurveyor(client)
    try:
        if seed:
            await seed_hosts_from_nodes(G, H)
        return await survey_hosts(
            H,
            surveyor,
            max_age=timedelta(seconds=max_age),
            max_workers=max_workers,
            limit=limit,
        )
    finally:
        await client.aclose()
        await pool.close()


if __name__ == "__main__":
    import os
    import sys
    import argparse
    from pytimeparse2 import parse

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(
        description="Survey Fediverse hosts for liveness and stats"
    )

    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (env: DATABASE_URL)",
    )

    parser.add_argument(
        "--max-age",
        default=DEFAULT_MAX_AGE,
        help=f"Max age before re-querying; default {DEFAULT_MAX_AGE}",
    )

    parser.add_argument(
        "--max-workers",
        default=DEFAULT_MAX_WORKERS,
        help=f"Max parallel workers; default {DEFAULT_MAX_WORKERS}",
        type=int,
    )

    parser.add_argument(
        "--limit",
        default=DEFAULT_LIMIT,
        help=f"Limit number of hosts to process; default {DEFAULT_LIMIT}",
        type=int,
    )

    parser.add_argument(
        "--no-seed",
        dest="seed",
        action="store_false",
        help="skip seeding the host table from node labels",
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    max_age_seconds = parse(args.max_age)
    if max_age_seconds is None:
        print(f"Invalid max age {args.max_age}")
        sys.exit(1)

    count = asyncio.run(
        main(
            args.database_url,
            max_age_seconds,
            args.max_workers,
            args.limit,
            args.seed,
        )
    )

    print(f"{count} surveyed")
