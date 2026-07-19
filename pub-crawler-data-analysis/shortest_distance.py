import logging
import random
import math
from collections import Counter

from load_graph import load_graph

DEFAULT_K = 500

def cumulcount(hist, cap):
    cumul = 0
    for d, n in sorted(hist.items()):
        if d > cap:
            break
        cumul += n
    return cumul

def quantile(hist, q):
    count = sum(hist.values())
    target = count * q
    cumul = 0
    for d, n in sorted(hist.items()):
        cumul += n
        if cumul >= target:
            return (d - 1) + (target - (cumul - n)) / n
    return -1

def mean_shortest_distance_files(nodes_filename, edges_filename, k):
    G = load_graph(nodes_filename, edges_filename)
    logging.info(f"sampling {k} vertices")
    sources = random.sample(range(G.vcount()), k)
    logging.info(f"Calculating distances")
    hist = Counter()
    for v in sources:
        logging.info(f"Distances for vertex {v}")
        row = G.distances(source=[v], mode="all")[0]
        reachable = [d for d in row if not math.isinf(d) and d != 0]
        hist.update(int(d) for d in reachable)
    stats = dict()
    total = sum(d * n for d, n in hist.items())
    count = sum(hist.values())
    stats["mean"] = total / count
    stats["median"] = quantile(hist, 0.5)
    stats["p90"] = quantile(hist, 0.9)
    stats["pct_within_mean_floor"] = \
        cumulcount(hist, math.floor(stats["mean"])) / count
    stats["pct_within_mean_ceil"] = \
        cumulcount(hist, math.ceil(stats["mean"])) / count
    return stats

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]
    if len(sys.argv) <= 3:
        k = DEFAULT_K
    else:
        k = int(sys.argv[3])

    stats = mean_shortest_distance_files(nodes_filename, edges_filename, k)

    print(stats)
