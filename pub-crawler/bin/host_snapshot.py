import logging
from pub_crawler.database_host_survey import DatabaseHostSurvey
import asyncio
import asyncpg
import uvloop
import pyarrow as pa
import pyarrow.parquet as pq
import collections
from datetime import datetime

host_schema = pa.schema(
    [
        ("id", pa.int32()),
        ("hostname", pa.string()),
        ("last_fetch_date", pa.timestamp("s", tz="UTC")),
        ("failure", pa.string()),
        ("error_detail", pa.string()),
        ("nodeinfo_version", pa.string()),
        ("software_name", pa.string()),
        ("software_version", pa.string()),
        ("users_total", pa.int64()),
        ("users_active_month", pa.int64()),
        ("users_active_halfyear", pa.int64()),
        ("local_posts", pa.int64()),
        ("local_comments", pa.int64()),
    ]
)

MAX_HOST_BATCH = 100_000

MAX_INT64 = 2**63 - 1

DEFAULT_HOST_FILENAME = "activitypub-hosts.parquet"


async def snapshot_hosts(H, host_filename):
    int_props = [
        "users_total",
        "users_active_month",
        "users_active_halfyear",
        "local_posts",
        "local_comments",
    ]
    other_props = [
        "failure",
        "error_detail",
        "nodeinfo_version",
        "software_name",
        "software_version",
    ]
    batch = collections.defaultdict(list)
    total = 0
    with pq.ParquetWriter(host_filename, host_schema) as writer:
        async for id, hostname, props in H.all_hosts():
            batch["id"].append(id)
            batch["hostname"].append(hostname)
            last_fetch_date = None
            if "last_fetch_date" in props:
                try:
                    last_fetch_date = datetime.fromisoformat(props["last_fetch_date"])
                except Exception as e:
                    logging.warning(
                        f"host {hostname} has bad last_fetch_date value {props["last_fetch_date"]}"
                    )
                    last_fetch_date = None
            batch["last_fetch_date"].append(last_fetch_date)
            for prop in int_props:
                pvalue = props.get(prop)
                value = None
                if isinstance(pvalue, int):
                    value = pvalue
                elif isinstance(pvalue, str):
                    try:
                        value = int(pvalue)
                    except:
                        value = None
                else:
                    value = None
                if isinstance(value, int) and 0 <= value <= MAX_INT64:
                    batch[prop].append(value)
                else:
                    batch[prop].append(None)
            for prop in other_props:
                batch[prop].append(props.get(prop))
            if len(batch["id"]) >= MAX_HOST_BATCH:
                writer.write_batch(pa.record_batch(batch, schema=host_schema))
                total += len(batch["id"])
                logging.info(f"{total} hosts written")
                for col in batch.values():
                    col.clear()
        if len(batch["id"]) > 0:
            writer.write_batch(pa.record_batch(batch, schema=host_schema))
            total += len(batch["id"])
            logging.info(f"{total} hosts written")
            for col in batch.values():
                col.clear()
    return total


async def main(database_url, host_filename):
    pool = await asyncpg.create_pool(database_url)
    total = 0
    try:
        total = await snapshot_hosts(DatabaseHostSurvey(pool), host_filename)
    finally:
        await pool.close()
    return total


if __name__ == "__main__":
    import os
    import sys
    import argparse

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description="Snapshot host data to Parquet file")

    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL connection URL (env: DATABASE_URL)",
    )

    parser.add_argument(
        "--host-filename",
        default=DEFAULT_HOST_FILENAME,
        help=f"Filename to store hosts; default {DEFAULT_HOST_FILENAME}",
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Set DATABASE_URL environment variable or pass --database-url")
        sys.exit(1)

    count = asyncio.run(main(args.database_url, args.host_filename))

    print(f"{count} hosts written to {args.host_filename}")
