import logging

from load_graph import load_graph


def mean_shortest_distance_files(nodes_filename, edges_filename):
    G = load_graph(nodes_filename, edges_filename)
    logging.info("calculating mean shortest distance")
    mean_path = G.average_path_length(directed=False, unconn=True)
    logging.info("fit complete")
    return mean_path


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]

    fits = mean_shortest_distance_files(nodes_filename, edges_filename)

    print(fits)
