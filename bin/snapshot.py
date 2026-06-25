import logging
from pub_crawler.database_graph import DatabaseGraph
import asyncio
import asyncpg
import uvloop
import pyarrow as pa
import pyarrow.parquet as pq
import collections
from datetime import datetime

node_schema = pa.schema(
    [
        ("id", pa.int32()),
        ("label", pa.string()),
        ("hostname", pa.string()),
        ("preferred_username", pa.string()),
        ("name", pa.string()),
        ("published", pa.timestamp("s", tz="UTC")),
        ("type", pa.string()),
        ("followers_count", pa.int32()),
        ("following_count", pa.int32()),
    ]
)

MAX_NODE_BATCH = 100_000

edge_schema = pa.schema(
    [
        ("from", pa.int32()),
        ("to", pa.int32()),
    ]
)

MAX_EDGE_BATCH = 1_000_000


async def snapshot_nodes(G, node_filename):
    max_id = -1
    other_props = [
        "hostname",
        "preferred_username",
        "name",
        "type",
        "followers_count",
        "following_count",
    ]
    batch = collections.defaultdict(list)
    total = 0
    with pq.ParquetWriter(node_filename, node_schema) as writer:
        async for id, label, props in G.all_nodes():
            max_id = max(id, max_id)
            batch["id"].append(id)
            batch["label"].append(label)
            published = None
            if "published" in props:
                try:
                    published = datetime.fromisoformat(props["published"])
                except Exception as e:
                    logging.warning(
                        f"node {id} has bad published value {props["published"]}"
                    )
                    published = None
            batch["published"].append(published)
            for prop in other_props:
                batch[prop].append(props.get(prop))
            if len(batch["id"]) >= MAX_NODE_BATCH:
                writer.write_batch(pa.record_batch(batch, schema=node_schema))
                total += len(batch["id"])
                logging.info(f"{total} nodes written")
                for col in batch.values():
                    col.clear()
        if len(batch["id"]) > 0:
            writer.write_batch(pa.record_batch(batch, schema=node_schema))
            total += len(batch["id"])
            logging.info(f"{total} nodes written")
            for col in batch.values():
                col.clear()
    return max_id


async def snapshot_edges(G, edge_filename, max_id):
    batch = collections.defaultdict(list)
    total = 0
    with pq.ParquetWriter(edge_filename, edge_schema) as writer:
        async for from_id, to_id, _ in G.all_edges():
            if from_id > max_id or to_id > max_id:
                continue
            batch["from"].append(from_id)
            batch["to"].append(to_id)
            if len(batch["from"]) >= MAX_EDGE_BATCH:
                writer.write_batch(pa.record_batch(batch, schema=edge_schema))
                total += len(batch["from"])
                logging.info(f"{total} edges written")
                for col in batch.values():
                    col.clear()
        if len(batch["from"]) > 0:
            writer.write_batch(pa.record_batch(batch, schema=edge_schema))
            total += len(batch["from"])
            logging.info(f"{total} edges written")
            for col in batch.values():
                col.clear()


async def snapshot(G, node_filename, edge_filename):

    max_id = await snapshot_nodes(G, node_filename)
    await snapshot_edges(G, edge_filename, max_id)


async def main(database_url, node_filename, edge_filename):
    pool = await asyncpg.create_pool(database_url)
    try:
        await snapshot(DatabaseGraph(pool), node_filename, edge_filename)
    finally:
        await pool.close()


if __name__ == "__main__":
    import os
    import sys

    uvloop.install()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for name in ("hpack", "h2", "httpcore", "httpx"):
        logging.getLogger(name).setLevel(logging.WARNING)

    node_filename = sys.argv[1]
    edge_filename = sys.argv[2]

    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("Set DATABASE_URL environment variable")
        exit(-1)

    asyncio.run(main(database_url, node_filename, edge_filename))
