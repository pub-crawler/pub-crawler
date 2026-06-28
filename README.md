# pub-crawler-data-analysis

Data analysis for Fediverse GML files

This is a set of Python scripts for analysing the data from the PubCrawler Fediverse crawl.

## Table of Contents

- [Install](#install)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Install

This package is managed with [uv](https://docs.astral.sh/uv/).

To make sure the libraries are all installed and read, run `uv sync`.

The `./data` directory is git-ignored, so that's probably a good place to put data.

## Usage

To run these in the right virtual environment, use this command:

```bash
uv run python <name-of-script.py> <arg1> <arg2> <arg3> ...
```

Loading big GML files takes a few minutes, but it eventually works.

### median_degree_by_depth.py [gml-file]

Calculates the median degree of the nodes in the graph, grouped by the depth of the crawl at which the node was found.

### top_n_no_share_hostnames.py [gml-file]

Finds nodes in the graph that didn't include outgoing or incoming edges, and groups them by hostname.

## gml_explorer.py
The GML Explorer currently provides the following utilities:

- **Preview GML File** – Display the first *N* lines of a GML file without loading the entire graph into memory. Useful for inspecting the file format and available attributes.
- **Top-Level GML Statistics** – Load a GML file and generate a high-level summary of the graph, including:
  - Basic graph statistics (nodes, edges, density, etc.)
  - Degree statistics
  - Connectivity statistics
  - Dataset metadata distributions (crawl depth, hostname, server software, HTTP status, discoverability, and indexability)
- **Create Test GML Sample** – Generate a small GML containing the first *N* nodes of a graph for rapid development and debugging. This is intended only as a lightweight testing utility and **not** as a representative network sample.

Planned additions include:

- **Snowball GML Sampler** – Generate representative subgraphs by performing a breadth-first (snowball) expansion from one or more seed actors.
- **Jupyter Notebook Analysis** – Exploratory notebooks for baseline network characterization, visualization, and graph metric analysis.

## Contributing

Just add a script to the top-level directory.

[networkx](https://networkx.org/en/) has a tonne of cool functionality but loads everything into memory, and might crash your and my laptops.

[igraph](https://python.igraph.org/en/stable/) is used for the scripts here, and probably will be the best for working with a big graph.

## License

GPLv3, the contributors.
