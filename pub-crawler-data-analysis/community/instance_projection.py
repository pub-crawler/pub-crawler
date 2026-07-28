import numpy as np
import pandas as pd
import logging
import pyarrow.parquet as pq

# projects the actor graph into an instance graph by aggregation. 
def instance_projection(nodes_filename, edges_filename, output_filename):
    # load id -> hostname map
    nodes = pd.read_parquet(nodes_filename, columns=["id", "hostname"])

    host = nodes["hostname"].astype("category")
    ids = nodes["id"].to_numpy().astype(np.int32)
    codes = host.cat.codes.to_numpy().astype(np.int32)
    names = host.cat.categories

    # build  a lookup array
    lut = np.full(ids.max() + 1, -1, dtype=np.int32)
    lut[ids] = codes

    def lookup(endpoints):
        # ids outside the lut range map to -1 instead of raising IndexError, or
        # (for negatives) wrapping around to some unrelated hostname
        in_range = (endpoints >= 0) & (endpoints < lut.size)
        return np.where(in_range, lut[np.where(in_range, endpoints, 0)], -1)

    # stream edges by row group

    parts = []
    dropped = 0
    pf = pq.ParquetFile(edges_filename)
    for i, batch in enumerate(
        pf.iter_batches(batch_size=1_000_000, columns=["from", "to"])
    ):
        frm = batch.column("from").to_numpy(zero_copy_only=False)
        to = batch.column("to").to_numpy(zero_copy_only=False)
        src, dst = lookup(frm), lookup(to)
        keep = (src >= 0) & (dst >= 0)  # unknown id, or hostname is null
        key = (src[keep].astype(np.int64) << 32) | dst[keep]
        logging.info(f"batch {i}: {len(frm):,} edges, {keep.sum():,} kept")
        dropped += len(frm) - int(keep.sum())

        parts.append(np.unique(key, return_counts=True))

    keys = np.concatenate([u for u, _ in parts])
    cnts = np.concatenate([c for _, c in parts])

    merged = (
        pd.DataFrame({"key": keys, "weight": cnts})
        .groupby("key", as_index=False)["weight"]
        .sum()
    )

    k = merged["key"].to_numpy()
    src_code = (k >> 32).astype(np.int32)
    dst_code = (k & 0xFFFFFFFF).astype(np.int32)
    # self-loops are intra-instance follows: kept, but flagged so they can be
    # excluded from the edge list used for layout
    out = pd.DataFrame(
        {
            "src_hostname": names[src_code],
            "dst_hostname": names[dst_code],
            "weight": merged["weight"],
            "self_loop": src_code == dst_code,
        }
    )
    out.to_parquet(output_filename, index=False)
    total = int(out["weight"].sum()) + dropped
    logging.info(f"{total:,} edges accounted for ({dropped:,} dropped)")
    assert total == pf.metadata.num_rows, f"{total:,} != {pf.metadata.num_rows:,}"


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)
    instance_projection(sys.argv[1], sys.argv[2], sys.argv[3])
