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

```mermaid
classDiagram
    class Crawler {
        +int max_workers
        +start()
        +finish()
        +abort()
    }
    class Worker {
        <<coroutine>>
        +str name
        +run()
    }
    class Dispatcher {
        +set_handler(job_type, handler)
        +dispatch(job)
        +enqueue(job)
        +get() job
        +seen(job) bool
        +done(job)
        +fail(job)
        +expired() list
    }
    class Handler {
        <<abstract>>
        +handle(job)
        +next_available(job)
    }
    class WebfingerHandler {
        +handle(job)
        +next_available(job)
    }
    class ActorHandler {
        +handle(job)
    }
    class CollectionHandler {
        +int max_depth
        +handle(job)
        +next_available(job)
    }
    class PageHandler {
        +handle(job)
        +next_available(job)
    }
    class WebfingerClient {
        +get_actor_id(wf) actor_id
        +next_available(wf)
        +aclose()
    }
    class ActivityPubClient {
        +str key_id
        +get(url) json
        +get_with_headers(url)
        +next_available(url)
        +aclose()
    }
    class FixedWindowCounter {
        +int tokens
        +int window_ms
        +acquire(origin)
        +next_available(origin)
    }
    class DatabaseGraph {
        +ensure_node(label)
        +ensure_edge(from, to)
        +set_node_properties(label, props)
        +get_node_property(label, name)
        +all_nodes()
        +all_edges()
    }
    class BlockAllCookiesPolicy {
        <<httpx cookie policy>>
    }
    class Signature {
        <<module>>
        +signature_header(url, method, headers, key_id, key)
    }

    Crawler o-- Dispatcher
    Crawler "1" o-- "max_workers" Worker : spawns
    Worker --> Dispatcher : get / dispatch / done
    Dispatcher "1" o-- "*" Handler : registry by job_type
    Handler <|-- WebfingerHandler
    Handler <|-- ActorHandler
    Handler <|-- CollectionHandler
    Handler <|-- PageHandler
    Handler --> Dispatcher : enqueues follow-on jobs

    WebfingerHandler --> WebfingerClient
    ActorHandler --> ActivityPubClient
    CollectionHandler --> ActivityPubClient
    PageHandler --> ActivityPubClient

    WebfingerHandler --> DatabaseGraph
    ActorHandler --> DatabaseGraph
    CollectionHandler --> DatabaseGraph
    PageHandler --> DatabaseGraph

    WebfingerClient "1" o-- "2" FixedWindowCounter : general, burst
    ActivityPubClient "1" o-- "3" FixedWindowCounter : general, paged, burst
    WebfingerClient ..> BlockAllCookiesPolicy
    ActivityPubClient ..> BlockAllCookiesPolicy
    ActivityPubClient ..> Signature
```

The jobs that flow through the queue, and the graph they build, are conceptual
classes even though jobs are plain JSON and nodes/edges are property-graph rows.

```mermaid
classDiagram
    class Job {
        <<abstract, JSON>>
        +str job_type
    }
    class WebfingerJob {
        +str webfinger
    }
    class ActorJob {
        +str actor_id
        +int depth
    }
    class CollectionJob {
        +str collection_id
        +str owner_id
        +str direction
        +int depth
    }
    class PageJob {
        +str page_id
        +str owner_id
        +str direction
        +int depth
    }
    class Node {
        <<property-graph row>>
        +int id
        +str label
        +str type
        +str preferred_username
        +str name
        +int followers_count
        +int following_count
        +bool discoverable
        +bool indexable
        +datetime last_fetch_date
        +... other profile properties
    }
    class Edge {
        <<property-graph row>>
        +int from_node
        +int to_node
        +bool from_followers
        +bool from_following
    }

    Job <|-- WebfingerJob
    Job <|-- ActorJob
    Job <|-- CollectionJob
    Job <|-- PageJob

    Node "1" --> "*" Edge : from_node
    Node "1" --> "*" Edge : to_node

    Dispatcher ..> Job : queues & routes by job_type
    Handler ..> Job : handles
    DatabaseGraph ..> Node : persists
    DatabaseGraph ..> Edge : persists
```

## Development

## Physical
