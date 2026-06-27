import igraph
import logging


def pickle_info(pickle_filename):
    logging.info(f"Reading pickle file {pickle_filename}")
    G = igraph.Graph.Read_Pickle(pickle_filename)
    logging.info("Read complete")
    return {
        'V': G.vcount(),
        'E': G.ecount(),
        'is_directed': G.is_directed(),
        'V_attrs': G.vs.attributes(),
        'E_attrs': G.es.attributes()
    }

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    pickle_filename = sys.argv[1]

    info = pickle_info(pickle_filename)

    print(info)
