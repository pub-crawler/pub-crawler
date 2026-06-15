# Pub Crawler

[![standard-readme compliant](https://img.shields.io/badge/readme%20style-standard-brightgreen.svg?style=flat-square)](https://github.com/RichardLitt/standard-readme)

A crawler for the social graph of the ActivityPub network

This software carefully crawls fetches actors and social graph connections using the ActivityPub standard. It respects rate limits from servers and privacy considerations from actors.

## Table of Contents

- [Security](#security)
- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Maintainers](#maintainers)
- [Contributing](#contributing)
- [License](#license)

## Security

Security issues with Pub Crawler can be reported directly to <mailto:evanp@gatech.edu>.

## Background

ActivityPub servers are often crawled at the server level, which gives aggregate information about actors on the server, but not specific information for each actor.

Pub Crawler uses ActivityPub itself to fetch actors then follow their social graph connections to other actors.

## Install

The software can be installed from GitHub at
<https://github.com/pub-crawler/pub-crawler>.

Note that the `public` directory includes specific URLs for the default software. New installations need to create a new public/private keypair, update the URLs the `public` directory, and deploy to a static Web server.

The software requires running a Postgres server for graph storage, and a Redis server for queue storage.

## Usage

The crawler has three distinct steps for running. A composite script is provided to run all three together, as well as a test script for fetching single ActivityPub objects.

### add_seeds.py

### crawl.py

### snapshot.py

### main.py

### fetch.py

## Maintainers

[@evanp](https://github.com/evanp) Evan Prodromou

## Contributing

Contributions are welcome. The public repository for issues and pull requests is
at <https://github.com/pub-crawler/pub-crawler>. Please file an issue or open a
pull request there.

## License

[GPL-3.0-or-later](LICENSE) © 2026 Evan Prodromou

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program. If not, see <https://www.gnu.org/licenses/>.
