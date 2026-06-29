import pandas
import igraph
import logging


def parquet_to_pickle(nodes_filename, edges_filename, pickle_filename):
    logging.info(f"Reading nodes file {nodes_filename}")
    nodes_df = pandas.read_parquet(nodes_filename)
    nodes_df.rename(columns={'name': 'display_name'}, inplace=True)
    logging.info(f"Reading edges file {edges_filename}")
    edges_df = pandas.read_parquet(edges_filename)
    logging.info("Initializing graph")
    G = igraph.Graph.DataFrame(
        edges_df, vertices=nodes_df, directed=True, use_vids=False
    )
    logging.info(f"Writing graph to pickle file {pickle_filename}")
    G.write_pickle(pickle_filename)
    logging.info("write complete")


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]
    pickle_filename = sys.argv[3]

    parquet_to_pickle(nodes_filename, edges_filename, pickle_filename)
