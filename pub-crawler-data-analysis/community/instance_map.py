""" instance level community map of the fediverse.
Workflow is to aggregate instance backbone > coverage filgter > mutual-follow backbown > Leiden communities > force-directed figure"""

import sys
import math
import collections
import numpy as np
import random
import pandas as pd
import igraph as ig
import leidenalg
import pyarrow.parquet as pq
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection


SEED = 42
colors = ["#E69F00", "#56B4E9", "#009E73", "#CC79A7",
             "#0072B2", "#D55E00", "#F0E442", "#444444"]

def instance_sizes(nodes_filename):
    """hostname -> crawled actor count. One column scan; cache to parquet."""
    # iter_batches over ["hostname"], Counter.update, -> pd.Series
    # 29,037 hostnames on the current snapshot
    counts = collections.Counter()
    for batch in pq.ParquetFile(nodes_filename).iter_batches(columns=["hostname"]):
        counts.update(batch.column("hostname").to_pandas().dropna())
    return pd.Series(counts)

def build_graph(backbone_filename, nodes_filename, min_actors=100, top_k=8):
    """Filtered, pruned, weighted instance graph."""

    sizes = instance_sizes(nodes_filename)
    keep = set (sizes[sizes >= min_actors].index)

    b = pd.read_parquet(backbone_filename)
    b = b[~b.self_loop]
    b = b[b.src_hostname.isin(keep) & b.dst_hostname.isin(keep)]

    # directed weight lookup, then keep pairs present in both directions 
    w = {(r.src_hostname, r.dst_hostname): r.weight for r in b.itertuples(index=False)}
    pair = {}
    for (a,d), weight in w.items():
        if a == d or (d,a) not in w:
            continue
        pair[tuple(sorted((a,d)))] = weight + w[(d,a)]
    # top_k strongest mutual edges per node (union over both endpoints)
    by_node = collections.defaultdict(list)
    for (a, d), wt in pair.items():
        by_node[a].append((wt, d))
        by_node[d].append((wt, a))
    kept = set()
    for node, lst in by_node.items():
        for wt, other in sorted(lst, reverse=True)[:top_k]:
            kept.add(tuple(sorted((node, other))))

    edges = pd.DataFrame(
        [(a, d, math.log1p(pair[(a, d)])) for (a, d) in kept],
        columns=["src_hostname", "dst_hostname", "weight"],
    )
    edges = edges.sort_values(["src_hostname", "dst_hostname"]).reset_index(drop=True)
    g = ig.Graph.DataFrame(edges, directed=False, use_vids=False)
    g.vs["actors"] = [int(sizes[h]) for h in g.vs["name"]]

    comp = g.connected_components()
    return g.subgraph(max(comp, key=len))

def detect(g):
    """Leiden on the undirected collapse. Returns (partition, undirected graph)."""

    return leidenalg.find_partition( g, leidenalg.ModularityVertexPartition, weights="weight", seed=SEED, n_iterations=-1)   
def community_table(g, part):
    """Per-community table: size, actors, modularity contribution, top instances.

    q_c = L_c/m - (k_c/2m)^2  (weighted, undirected); the q_c sum to part.modularity.
    Communities are renumbered by instance count (0 = largest).
    """
    w = np.array(g.es["weight"], float)
    mem = np.array(part.membership)
    order = pd.Series(mem).value_counts().index.tolist()
    remap = {old: new for new, old in enumerate(order)}
    mem = np.array([remap[m] for m in mem])
    K = len(order)

    m_tot = w.sum()
    strength = np.array(g.strength(weights=g.es["weight"]))
    src = np.array([e.source for e in g.es])
    dst = np.array([e.target for e in g.es])
    internal = mem[src] == mem[dst]

    actors = np.array(g.vs["actors"])
    names = np.array(g.vs["name"])
    rows = []
    for c in range(K):
        L_c = w[internal & (mem[src] == c)].sum()
        k_c = strength[mem == c].sum()
        q_c = L_c / m_tot - (k_c / (2 * m_tot)) ** 2
        idx = np.where(mem == c)[0]
        top = names[idx[np.argsort(-actors[idx])][:4]]
        rows.append({
            "community": c,
            "n_instances": len(idx),
            "n_actors": int(actors[idx].sum()),
            "modularity_contribution": round(float(q_c), 4),
            "top_instances": ", ".join(top),
        })
    return pd.DataFrame(rows), mem


def draw(g, mem, output_filename, part):
    """Force-directed figure: color = community, size = log actors."""
    w = g.es["weight"]
    vcolor = [colors[m] if m < 8 else "#cccccc" for m in mem]
    actors = np.array(g.vs["actors"], float)
    la = np.log1p(actors)
    vsize = 5 + 20 * (la - la.min()) / (la.max() - la.min())

    random.seed(SEED)
    coords = np.array(g.layout_fruchterman_reingold(weights=w, niter=1500).coords)

    fig, ax = plt.subplots(figsize=(15, 15), dpi=110)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")
    segs = coords[[(e.source, e.target) for e in g.es]]
    ax.add_collection(LineCollection(segs, colors="#9aa0a6", linewidths=0.12, alpha = 0.10, zorder=1))
    ax.scatter(coords[:, 0], coords[:, 1], s=(vsize ** 2) / 3,c=vcolor,
               edgecolors="white", linewidths=0.35, zorder=2)
    for i in np.argsort(-actors)[:16]:
        ax.annotate(g.vs[i]["name"], (coords[i, 0], coords[i,1]), fontsize=8,
                    ha="center", va="center", zorder=3,
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.7))
    ax.set_axis_off()
    ax.set_aspect("equal")
    Q = g.modularity(part.membership, weights=g.es["weight"])
    ax.set_title(f"Fediverse instance communities — mutual-follow backbone\n"
                 f"Leiden (seed {SEED}): {len(part)} communities, "
                 f"modularity Q = {Q:.3f}", fontsize=13)
    fig.tight_layout()
    fig.savefig(output_filename, facecolor="white", bbox_inches="tight")


if __name__ == "__main__":
    backbone_filename, nodes_filename, output_filename = sys.argv[1:4]

    g = build_graph(backbone_filename, nodes_filename)
    part = detect(g)
    table, mem = community_table(g, part)

    pd.set_option("display.width", 170, "display.max_colwidth",60)
    Q = g.modularity(part.membership, weights=g.es["weight"])
    print(f"Leiden seed={SEED}: {len(part)} communities, "
          f"modularity Q = {Q:.4f}\n")
    print(table.to_string(index=False))
    table.to_csv(output_filename.rsplit(".", 1)[0] + "-communities.csv", index=False)

    draw(g, mem, output_filename, part)
    print(f"\nwrote {output_filename} and its -communities.csv")