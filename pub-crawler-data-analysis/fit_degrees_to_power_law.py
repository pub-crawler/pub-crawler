import pandas
import igraph
import logging
import powerlaw


def fit_degrees_to_power_law(G):
    logging.info("Getting in-degree values")
    in_degrees = G.degree(mode="in")
    logging.info("Calculating in-degree fit")
    fits = {}
    fit = powerlaw.Fit(in_degrees, discrete=True)
    logging.info("Comparing power_law to lognormal")
    R, p = fit.distribution_compare("power_law", "lognormal", normalized_ratio=True)
    fits["in"] = {
        "alpha": fit.power_law.alpha,
        "xmin": fit.power_law.xmin,
        "R": R,
        "p": p
    }
    return fits


def fit_degrees_to_power_law_files(nodes_filename, edges_filename):
    logging.info(f"Reading nodes file {nodes_filename}")
    nodes_df = pandas.read_parquet(nodes_filename)
    nodes_df.drop(columns=['name'], inplace=True)
    logging.info(f"Reading edges file {edges_filename}")
    edges_df = pandas.read_parquet(edges_filename)
    logging.info("Initializing graph")
    G = igraph.Graph.DataFrame(
        edges_df, vertices=nodes_df, directed=True, use_vids=False
    )
    logging.info("fitting to power law")
    fits = fit_degrees_to_power_law(G)
    logging.info("fit complete")
    return fits


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    nodes_filename = sys.argv[1]
    edges_filename = sys.argv[2]

    fits = fit_degrees_to_power_law_files(nodes_filename, edges_filename)

    print(fits)
