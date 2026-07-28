import igraph as ig, pandas as pd, collections, pyarrow.parquet as pq

def instance_sizes(nodes_filename):
    c = collections.Counter()
    for b in pq.ParquetFile(nodes_filename).iter_batches(columns=["hostname"]):
        c.update(b.column("hostname").to_pandas().dropna())
    return pd.Series(c)

def export(backbone, nodes, out, min_actors=100, top_k=5):
    sizes = instance_sizes(nodes)
    keep  = sizes[sizes >= min_actors].index
    b = pd.read_parquet(backbone)
    b = b[~b.self_loop]
    b = b[b.src_hostname.isin(keep) & b.dst_hostname.isin(keep)]
    b = b.sort_values("weight", ascending=False).groupby("src_hostname").head(top_k)
    g = ig.Graph.DataFrame(b, directed=True, use_vids=False)
    g.vs["actors"] = [int(sizes[h]) for h in g.vs["name"]]
    g.write_graphml(out)
    print(g.summary())