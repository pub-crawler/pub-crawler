# Architecture of PubCrawler

This is a [4+1 architectural view model](https://en.wikipedia.org/wiki/4%2B1_architectural_view_model) for the PubCrawler system.

## Use cases

### Personae

- Crawler operator: runs the crawl
- Data analyst: determines properties of the network
- Server operator: runs a server
- Fediverse user: has an account on the Fediverse

```mermaid
flowchart LR
    operator["Crawler Operator"]
    admin["Server Admin"]
    fediuser["Fediverse User"]
    analyst["Data Analyst"]

    subgraph Wikidata
        queryWikidata(["Query seeds"])
    end

    subgraph PubCrawler
        seed(["Seed a crawl"])
        crawl(["Crawl the social graph"])
        createSnapshot(["Create snapshot"])
        restartCrawl(["Restart crawl with progress"])
    end

    subgraph FediverseServer
        identify(["Identify the crawler"])
        ratelimit(["Limit request rate of crawler"])
        restrictGraph(["Disallow access to social graph"])
        restrictProfile(["Disallow storage of personal profile"])
    end

    subgraph Snapshot
        queryGraph(["Query graph properties"])
        estimate(["Estimate full graph properties"])
    end

    operator --- queryWikidata
    operator --- seed
    operator --- crawl
    operator --- createSnapshot
    operator --- restartCrawl

    analyst --- queryGraph
    analyst --- estimate

    admin --- identify
    admin --- ratelimit

    fediuser --- restrictGraph
    fediuser --- restrictProfile
```

## Process

## Logical

## Development

## Physical
