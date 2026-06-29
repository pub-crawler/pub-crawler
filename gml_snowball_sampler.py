import igraph as ig

# TODO     
#    From Slack recommendation: 
#    Develop a script to tear out a representative-ish sample. 
#    One way to do that is to start with a depth=0 seed actor, find all their connections, 
#    and then all the next level's connections, out to some depth like 2 or 3 or 4.

def gml_snowball_sampler(g: ig.Graph, start_node: int, hop_depth: int = 2) -> ig.Graph:
    """
    Perform a snowball sampling of a GML graph starting from a given node.

    Parameters
    ----------
    g : ig.Graph
        Loaded igraph graph.
    start_node : int
        The index of the node to start the snowball sampling from.
    hop_depth : int
        The maximum number of hops to include in the sampled graph.
        Default is 2, which means the sampled graph will include the start node, its immediate neighbors, and their neighbors.
    """
    pass