import pandas
import igraph
import logging

def load_graph(nodes_filename, edges_filename):
    logging.info(f"Reading nodes file {nodes_filename}")
    nodes_df = pandas.read_parquet(nodes_filename)
    logging.info(f"Reading edges file {edges_filename}")
    edges_df = pandas.read_parquet(edges_filename)
    logging.info("Initializing graph")
    return igraph.Graph.DataFrame(
        edges_df, vertices=nodes_df.drop(columns=['name']), directed=True, use_vids=False
    )