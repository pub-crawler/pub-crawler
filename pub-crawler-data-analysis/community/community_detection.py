import logging
import igraph as ig
import leidenalg 
from load_graph import load_graph


def partition(G):
    # Use the Leiden algorithm to detect communities
    part = leidenalg.find_partition(G, leidenalg.ModularityVertexPartition)
    return part

# for each detected community, print the number of nodes and instances it spans. 
def community(nodes_filename, edges_filename):
    G = load_graph(nodes_filename, edges_filename)
    part = partition(G)
    for i, community_nodes in enumerate(part):
        instances = set()
        logging.info(f"Community {i}: {len(community_nodes)} nodes")
        for node in community_nodes:
            node = G.vs[node]
            instances.add(node["hostname"])

        logging.info(f"Community {i}: {len(community_nodes)} nodes, spans {len(instances)} instances")
        num_instances = len(part)
    return num_instances


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]

    num_comms = community(nodes_filename, edges_filename)

    print(num_comms)