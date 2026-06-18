import logging
from pub_crawler.database_graph import DatabaseGraph
import asyncio
import asyncpg


def _escape(s):
    out = []
    for c in s:
        o = ord(c)
        if c == "&":
            out.append("&#38;")
        elif c == '"':
            out.append("&#34;")
        elif o < 0x20 or o > 0x7E:
            out.append(f"&#{o};")
        else:
            out.append(c)
    return "".join(out)


async def snapshot(G, output_filename):
    with open(output_filename, "w") as f:
        f.write("graph [\n")
        f.write("  directed 1\n")
        max_id = -1

        async for id, label, props in G.all_nodes():
            max_id = max(id, max_id)
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
            # skip edges to nodes that were added while we were iterating
            if from_node > max_id or to_node > max_id:
                continue
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
    pool = await asyncpg.create_pool(database_url)
    try:
        await snapshot(DatabaseGraph(pool), output_filename)
    finally:
        await pool.close()


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
