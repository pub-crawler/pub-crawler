import logging
from pub_crawler.database_graph import DatabaseGraph
import asyncio
import asyncpg


def _escape(value):
    return value.replace("&", "&#38;").replace('"', "&#34;")


async def snapshot(G, output_filename):
    with open(output_filename, "w") as f:
        f.write("graph [\n")
        f.write("  directed 1\n")

        async for id, label, props in G.all_nodes():
            f.write("  node [\n")
            f.write(f"    id {id}\n")
            f.write(f'    label "{_escape(label)}"\n')
            for name, value in props.items():
                if isinstance(value, str):
                    f.write(f'    {name} "{_escape(value)}"\n')
                elif isinstance(value, bool):
                    f.write(f"    {name} {1 if value else 0}\n")
                elif isinstance(value, int):
                    f.write(f"    {name} {value}\n")
            f.write("  ]\n")

        async for from_node, to_node, props in G.all_edges():
            f.write("  edge [\n")
            f.write(f"    source {from_node}\n")
            f.write(f"    target {to_node}\n")
            for name, value in props.items():
                if isinstance(value, str):
                    f.write(f'    {name} "{_escape(value)}"\n')
                elif isinstance(value, bool):
                    f.write(f"    {name} {1 if value else 0}\n")
                elif isinstance(value, int):
                    f.write(f"    {name} {value}\n")
            f.write("  ]\n")

        f.write("]\n")


async def main(database_url, output_filename):
    conn = await asyncpg.connect(database_url)
    try:
        async with conn.transaction():
            await snapshot(DatabaseGraph(conn), output_filename)
    finally:
        await conn.close()


if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("httpcore").setLevel(logging.INFO)

    output_filename = sys.argv[1]

    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("Set DATABASE_URL environment variable")
        exit(-1)

    asyncio.run(main(database_url, output_filename))
