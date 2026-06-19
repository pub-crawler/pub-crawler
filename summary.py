import igraph

def main(gml_filename):
    g = igraph.Graph.Read_GML(gml_filename)
    print(g.summary())


if __name__ == "__main__":
    import sys
    gml_filename = sys.argv[1]
    main(gml_filename)
