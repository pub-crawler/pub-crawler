import igraph
import statistics
from collections import defaultdict


def median_degree_by_depth(g):
    degrees = g.degree(mode="all")
    depths = g.vs["depth"]
    groups = defaultdict(list)
    for i in range(len(depths)):
        if depths[i] is None or depths[i] != depths[i]:
            continue
        groups[int(depths[i])].append(degrees[i])
    result = {}
    for depth, depth_degrees in groups.items():
        result[depth] = statistics.median(depth_degrees)
    return result


def median_degree_by_depth_file(gml_filename):
    g = igraph.Graph.Read_GML(gml_filename)
    dbd = median_degree_by_depth(g)
    for depth, median_degree in dbd.items():
        print(f"Depth: {depth} Median degree: {median_degree}")


if __name__ == "__main__":
    import sys

    gml_filename = sys.argv[1]
    median_degree_by_depth_file(gml_filename)
