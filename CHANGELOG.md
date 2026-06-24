# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.1] - 2026-06-24

### Fixed

- Bad commands in the Dockerfile made the image too large to build.
- Leave some unused libarrow libraries out of the build.

## [0.6.0] - 2026-06-24

### Changed

- snapshot.py now outputs two Parquet files; one for nodes and one for edges.

## [0.5.5] - 2026-06-22

### Fixed

- Ignore all cookies from servers so the client doesn't spend huge amounts of time looking up cookies.

## [0.5.4] - 2026-06-21

### Fixed

- Only parse PEM data once per crawl, rather than
  once per request

## [0.5.3] - 2026-06-21

### Changed

- Set database connections maximum to one half of max_workers.

### Fixed

- Set default log output to info, and make sure HTTP
data is only at warning level. Job handling moves from debug to info.

## [0.5.2] - 2026-06-21

### Changed

- Change WebfingerClient, ActivityPubClient keepalive expiry to matches burst window
- Use up to max_workers db connections in asyncpg pool
- Use up to max_workers connections and
keepalive connections in ActivityPubClient and WebfingerClient
- use uvloop
- use orjson
- Use http2 for Web clients

## [0.5.1] - 2026-06-18

### Fixed

- Snapshot script doesn't emit edge stanzas for unmatched node stanzas. This can be
  a problem if the crawl is happening while the snapshot is being taken.

## [0.5.0] - 2026-06-17

### Added

- Fixup script to reprioritise the queue based on depth and type.

### Changed

- Dispatcher prioritises the queue by depth and type of the job as well as next-available-execution time.

## [0.4.1] - 2026-06-16

### Fixed

- Missing fixup_seen.py in Dockerfile

## [0.4.0] - 2026-06-16

### Fixed

- Better robustness in the crawler for errors from the dispatcher.
  Should prevent workers dying off in the crawler. Includes exponential
  backoff so that high request volume to Redis doesn't kill the whole
  crawl.

### Added

- Deduplication of jobs before adding them to the queue. Should
  greatly reduce queue size and job contention.
- fixup_seen.py script, run once to initialize the seen-jobs
  data structure, crawler can't be running.

## [0.3.4] - 2026-06-15

### Fixed

- Better handling for `next`/`first` page links that are
  objects, not strings.
- Better handling for HTTP responses that aren't JSON.

## [0.3.3] - 2026-06-15

### Fixed

- Correctly record when a collection has zero total items.

### Changed

- Added bulk update and query methods to DatabaseGraph and used them
  to reduce the number of database calls in ActorHandler, CollectionHandler,
  and PageHandler.
- Added cache for node ids in DatabaseGraph, to reduce the number of
  database calls even further.

## [0.3.2] - 2026-06-15

### Fixed

- fixup_queue.py uses a cursor to keep resource requirements low and
  avoid timeouts

## [0.3.1] - 2026-06-15

### Fixed

- fixup_queue.py was not in the Dockerfile

## [0.3.0] - 2026-06-15

### Added

- Clean(er) shutdown on SIGTERM
- Queue key reformat script
- Save failed jobs for manual recovery
- Reenqueue abandoned jobs

### Fixed

- Use datestamp rather than counter to order jobs in the queue

### Changed

- Crawl script renamed to crawl.py

## [0.2.0] - 2026-06-14

### Added

- Capture `alsoKnownAs` and `url` from actors.
- Capture additional actor properties.
- Capture actor `summary` and `icon`.

### Changed

- Respect consent flags when collecting data: skip profile data unless
  `discoverable` is `true`, and skip outbox data unless `indexable` is `true`.

## [0.1.0] - 2026-06-12

### Added

- ActivityPub crawler with a single `main` entry point that orchestrates the
  webfinger, actor, collection, and page handlers.
- WebFinger lookup and handler that enqueues an actor job.
- `ActorHandler`, `CollectionHandler`, and `PageHandler` wired into the queue.
- HTTP Signatures (draft-cavage-12) for signed requests via `ActivityPubClient`.
- `Dispatcher` with throttle-aware scheduling to avoid head-of-line blocking,
  in-flight job tracking, and `join()` support.
- Request throttling with a fixed-window counter and a burst bucket to keep
  requests smooth.
- HTTP transport with retries and `ActivityPubClient.get_with_headers()`.
- Graph storage in a Postgres database using asyncpg connection pools, plus a
  Redis-backed queue.
- Migrations runner (`run_migrations.py`) with a guard.
- Standalone crawler daemon and a `seeds` script.
- Snapshot of the graph to a GML file.
- Crawl metadata for pages, actor depth, hostname, and server software.
- Capture of `indexable` and `discoverable` actor flags.
- Command-line arguments for `main` and the crawler.
- Homepage for crawler.pub and a static signing actor served for crawls.
- Dockerfile.

### Fixed

- Don't enqueue actors that have already been fetched.
- Snapshot handles non-ASCII text better.
- Crawler max depth defaults to 1.
- `DatabaseGraph` no longer uses its own transaction.

[Unreleased]: https://github.com/pub-crawler/pub-crawler/compare/v0.6.1...HEAD
[0.6.1]: https://github.com/pub-crawler/pub-crawler/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.5...v0.6.0
[0.5.5]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/pub-crawler/pub-crawler/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/pub-crawler/pub-crawler/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/pub-crawler/pub-crawler/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/pub-crawler/pub-crawler/compare/v0.3.4...v0.4.0
[0.3.4]: https://github.com/pub-crawler/pub-crawler/compare/v0.3.3...v0.3.4
[0.3.3]: https://github.com/pub-crawler/pub-crawler/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/pub-crawler/pub-crawler/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/pub-crawler/pub-crawler/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/pub-crawler/pub-crawler/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pub-crawler/pub-crawler/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pub-crawler/pub-crawler/releases/tag/v0.1.0
