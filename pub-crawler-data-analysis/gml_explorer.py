import os
import igraph as ig
from itertools import islice
import time
import math

#------------------------------------------------------------
# LOAD GML UTIL FUNCTIONS
#------------------------------------------------------------
def format_file_size(path: str) -> str:
    """
    Return file size as a human-readable string.
    """
    size_bytes = os.path.getsize(path)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024

    return f"{size_bytes:.2f} PB"

def read_gml_with_status(path: str) -> ig.Graph:
    """
    Load a GML file while displaying file size and elapsed load time.
    """

    print(f"\nLoading {os.path.basename(path)}")
    print(f"File size: {format_file_size(path)}")
    print("This may take several minutes for large GML files...\n")

    start_time = time.perf_counter()

    g = ig.Graph.Read_GML(path)

    elapsed = time.perf_counter() - start_time

    print(f"Graph loaded successfully in {elapsed:.1f} seconds.\n")

    return g

#------------------------------------------------------------
# GML VIEWING AND INSPECTION FUNCTIONS
#------------------------------------------------------------
def preview_gml(path: str, num_lines: int = 80) -> None:
    """
    Print the first few lines of a GML file without loading it into memory.
    Useful for inspecting the schema and file structure.
    
    path: The path to the GML file.
    num_lines: The number of lines to preview from the file.
    """
    
    print(f"Previewing first {num_lines} lines of {path}\n")

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in islice(f, num_lines):
            print(line.rstrip())

def top_level_gml_stats(path: str, top_n: int = 10) -> None:
    """
    Load a GML file and print high-level statistics describing the graph.

    Parameters
    ----------
    path : str
        Path to the GML file.
    top_n : int
        Number of most common categorical values to display.
    """

    g = read_gml_with_status(path)

    print("=" * 60)
    print("Fediverse Graph Summary")
    print("=" * 60)

    # Basic graph information
    print("\nBasic Graph Statistics")
    print("General information describing the overall graph structure.")
    print("(v) Vertex attribute (stored on each node)")
    print("(e) Edge attribute (stored on each edge)")

    print(f"\n{g.summary()}")
    
    print("\nDirected: Indicates whether connections have direction. In the Fediverse, a follow"
          "from Alice to Bob is different from Bob following Alice.")
    print("Nodes: The total number of unique accounts in the graph.")
    print("Edges: The total number of connections between accounts in the graph.")
    print("Density: Measures how many of the possible connections actually exist. "
        "Values closer to 0 indicate a sparse network, while values closer to 1 "
        "indicate a highly connected network.")
    
    print(f"\nDirected:          {g.is_directed()}")
    print(f"Nodes:             {g.vcount():,}")
    print(f"Edges:             {g.ecount():,}")
    print(f"Density:           {g.density():.8f}")

    # Degree statistics
    print("\nDegree Statistics")
    print("-" * 60)
    print("A node's degree is the number of connections it has. "
    "Higher degree generally indicates a more connected account.")

    degree = g.degree()

    print(f"\nMinimum degree:    {min(degree)}")
    print(f"Maximum degree:    {max(degree)}")
    print(f"Average degree:    {sum(degree)/len(degree):.2f}")

    degree.sort()

    if len(degree) % 2 == 0:
        median = (degree[len(degree)//2] + degree[len(degree)//2-1]) / 2
    else:
        median = degree[len(degree)//2]

    print(f"Median degree:     {median}")


    # Connected components
    print("\nConnectivity")
    print("-" * 60)
    print("Connected components measure how fragmented the network is. "
    "A large connected component indicates many users are reachable through social connections.")
    print("Weak components ignore edge direction (A→B counts as connected)."
    "Strong components require mutual reachability following edge directions.\n")
    
    weak = g.connected_components(mode="weak")

    print(f"Weak components:           {len(weak)}")
    print(f"Largest component:         {max(weak.sizes()):,}")
    
    if g.is_directed():
        strong = g.connected_components(mode="strong")
        print(f"Strong components:         {len(strong)}")

    isolates = sum(d == 0 for d in degree)
    print(f"Isolated nodes:            {isolates:,}")


    # Dataset metadata
    print("\nDataset Characteristics")
    print("-" * 60)
    print("These summaries describe common metadata attached to user accounts collected during the crawl.")
    print("Crawl Depth: How far from the seed actor the account was discovered. Depth=0 is the seed actor, depth=1 are their direct connections, depth=2 are connections of connections, etc.")
    print("Hostname: The domain name of the Fediverse instance hosting the account.")
    print("Server: The software running the Fediverse instance (e.g., Mastodon, Pleroma, Misskey).")
    print("HTTP Status: The HTTP response code received when attempting to access the account's profile page.")
    print("Discoverable: Whether the account is discoverable through the instance's search functionality.")
    print("Indexable: Whether the account is indexable by search engines.")
    
    attribute_labels = {
        "depth": "Crawl Depth Distribution",
        "hostname": "Fediverse Instance Distribution",
        "server": "Server Software Distribution",
        "httpstatus": "HTTP Status Distribution",
        "discoverable": "Discoverability",
        "indexable": "Indexability",
    }

    for attribute in [
        "depth",
        "hostname",
        "server",
        "httpstatus",
        "discoverable",
        "indexable",
    ]:

        print(f"\n{attribute_labels[attribute]}")

        if attribute not in g.vs.attributes():
            print("Attribute not present.")
            continue

        values = {}

        for value in g.vs[attribute]:

            # Normalize missing values
            if value is None:
                value = "Unknown"
            elif isinstance(value, float) and math.isnan(value):
                value = "Unknown"

            values[value] = values.get(value, 0) + 1

        for value, count in sorted(
            values.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]:

            print(f"  {str(value):<30} {count:,}")

    print("\nDone.")

def create_test_sample(path: str, num_nodes: int = 50) -> None:
    """
    Create a small test GML containing the first N nodes.

    This utility is intended for rapid development and debugging of
    analysis functions. It is NOT intended to produce a representative
    sample of the Fediverse graph. For representative sampling, use
    the snowball sampler.
    """

    # TODO:
    # Replace this development utility with a true snowball sampler in gml_snowball_sampler.py
    # that preserves local network topology around a depth=0 seed actor.

    g = read_gml_with_status(path)

    original_nodes = g.vcount()  
    original_edges = g.ecount()

    n = min(num_nodes, original_nodes)

    print(f"\nCreating test sample containing the first {n:,} nodes...")

    sample = g.subgraph(range(n))

    # Create output directory if it doesn't already exist
    output_dir = "gml_test_samples"
    os.makedirs(output_dir, exist_ok=True)

    # Build output filename
    base_name = os.path.splitext(os.path.basename(path))[0]
    output_path = os.path.join(
        output_dir,
        f"{base_name}_test_{n}_nodes.gml"
    )

    print(f"\nWriting test sample to:\n{output_path}")

    sample.write_gml(output_path)

    print("\nTest sample created successfully!")
    print("-" * 50)
    print(f"Original graph : {original_nodes:,} nodes, {original_edges:,} edges")
    print(f"Test sample    : {sample.vcount():,} nodes, {sample.ecount():,} edges")
    print(f"Nodes retained : {sample.vcount() / original_nodes:.2%}")
    
#------------------------------------------------------------
# MAIN
#------------------------------------------------------------

def main():
    """
    Main function to run the Fediverse GML Network Explorer.
    """

    # Action dictionary mapping user choices to functions and prompts
    actions = {
        "1": {
            "label": "Preview GML file",
            "function": preview_gml,
            "prompt": "Number of lines to preview [default=80]: ",
            "default": 80,
        },
        "2": {
            "label": "Show top-level GML statistics",
            "function": top_level_gml_stats,
            "prompt": "Number of top attribute values to show [default=10]: ",
            "default": 10,
            "requires_loaded_graph": True,
        },
        "3": {
            "label": "Create test GML sample (first N nodes)",
            "function": create_test_sample,
            "prompt": "Number of nodes to include [default=50]: ",
            "default": 50,
        },
    }

    # Main loop 
    while True:
        print("-" * 50)
        print("Fediverse GML Explorer")
        print("-" * 50)
        print(r"""
      (\_/)
     ( •ᴗ•)  Digging through Fediverse GMLs...
    c(")_(")
        """)

        for key, action in actions.items():
            print(f"{key}. {action['label']}")
        print("0. Exit")
        print()

        choice = input("Select an option: ").strip()

        if choice == "0":
            print("\nGoodbye! •ᴗ•")
            break

        if choice not in actions:
            print("\nInvalid option. Please try again.\n")
            continue

        path = input("\nEnter path to GML file: ").strip()

        action = actions[choice]
        value = input(action["prompt"]).strip()
        value = int(value) if value else action["default"]

        try:
            action["function"](path, value)
        except FileNotFoundError:
            print(f"\nFile not found: {path}")
        except ValueError:
            print("\nPlease enter a valid number.")
        except Exception as e:
            print(f"\nSomething went wrong: {e}")

        input("\nPress Enter to return to the main menu...")

if __name__ == "__main__":
    main()