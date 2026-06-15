# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0]

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


[Unreleased]: https://github.gatech.edu/eprodromou3/pub-crawler/compare/v0.2.0...HEAD
[0.2.0]: https://github.gatech.edu/eprodromou3/pub-crawler/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.gatech.edu/eprodromou3/pub-crawler/releases/tag/v0.1.0
