import logging
import random
import math

from load_graph import load_graph

DEFAULT_K = 500

def mean_shortest_distance_files(nodes_filename, edges_filename, k):
    G = load_graph(nodes_filename, edges_filename)
    logging.info(f"sampling {k} vertices")
    sources = random.sample(range(G.vcount()), k)
    logging.info(f"Calculating distances")
    total_count = 0
    total_sum = 0
    for v in sources:
        logging.info(f"Distances for vertex {v}")
        row = G.distances(source=[v], mode="all")[0]
        finite = [d for d in row if not math.isinf(d)]
        total_count += len(finite) - 1 # self-distance
        total_sum += sum(finite)
    return total_sum / total_count


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]
    if len(sys.argv) <= 3:
        k = DEFAULT_K
    else:
        k = int(sys.argv[3])

    mean_path = mean_shortest_distance_files(nodes_filename, edges_filename, k)

    print(mean_path)
