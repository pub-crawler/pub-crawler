import collections
import logging

from load_graph import load_graph
from community_detection import partition

# load graph and detect communit detection
def edge_counts(G, part):
    membership = part.membership
    internal = collections.Counter()
    external_out = collections.Counter()
    external_in = collections.Counter()
    # classify every edge in graph relative to the community it touches
    for edge in G.es:
        src_comm = membership[edge.source]
        tgt_comm = membership[edge.target]
        if src_comm == tgt_comm:
            internal[src_comm] += 1
        else:
            external_out[src_comm] += 1
            external_in[tgt_comm] += 1
    return internal, external_out, external_in

# compute isolation ratio for each community. returns community size and how many distinct instances it spans.
def isolation_stats(nodes_filename, edges_filename):
    G = load_graph(nodes_filename, edges_filename)
    part = partition(G)
    internal, external_out, external_in = edge_counts(G, part)

    stats = []
    for i, community_nodes in enumerate(part):
        n_internal = internal[i]
        n_out = external_out[i]
        n_in = external_in[i]
        total = n_internal + n_out + n_in
        isolation_ratio = n_internal / total if total else 0.0
        instances = {G.vs[n]["hostname"] for n in community_nodes}
        stats.append(
            {
                "community": i,
                "size": len(community_nodes),
                "instances": len(instances),
                "internal_edges": n_internal,
                "external_in_edges": n_in,
                "external_out_edges": n_out,
                "isolation_ratio": isolation_ratio,
            }
        )
    return stats


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]

    stats = isolation_stats(nodes_filename, edges_filename)
    for s in sorted(stats, key=lambda s: s["isolation_ratio"], reverse=True):
        logging.info(s)
