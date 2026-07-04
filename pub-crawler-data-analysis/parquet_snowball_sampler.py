import logging
import pandas
import igraph

from load_graph import load_graph

def parquet_snowball_sampler(nodes_filename_in, edges_filename_in, seed, nodes_filename_out, edges_filename_out):
  username, host = seed.rsplit('@', 1)
  logging.info(f"Reading nodes file {nodes_filename_in}")
  nodes_df = pandas.read_parquet(nodes_filename_in)
  logging.info(f"Reading edges file {edges_filename_in}")
  edges_df = pandas.read_parquet(edges_filename_in)
  logging.info("Initializing graph")
  G = igraph.Graph.DataFrame(
      edges_df, vertices=nodes_df.drop(columns=['name']), directed=True, use_vids=False
  )
  logging.info(f"Finding seed node")
  v = G.vs.find(preferred_username=username, hostname=host)
  logging.info(f"getting neighbourhood")
  vs = G.neighborhood(vertices=v.index, order=1, mode="all")
  logging.info(f"getting id set")
  id_set = {G.vs[i]["name"] for i in vs}
  logging.info(f"getting subgraph vertex dataframe")
  sub_node_pd = nodes_df[nodes_df["id"].isin(id_set)]
  logging.info(f"writing nodes to {nodes_filename_out}")
  sub_node_pd.to_parquet(nodes_filename_out, index=False)
  logging.info(f"getting subgraph edge dataframe")
  sub_edge_pd = edges_df[(edges_df["from"].isin(id_set)) & (edges_df["to"].isin(id_set))]
  logging.info(f"Writing edges data to {edges_filename_out}")
  sub_edge_pd.to_parquet(edges_filename_out, index=False)

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename_in = sys.argv[1]
    edges_filename_in = sys.argv[2]

    seed = sys.argv[3]

    nodes_filename_out = sys.argv[4]
    edges_filename_out = sys.argv[5]

    parquet_snowball_sampler(nodes_filename_in, edges_filename_in, seed, nodes_filename_out, edges_filename_out)
