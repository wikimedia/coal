# Coal

Metrics processing and Web API for the Wikimedia Performance Team.

See https://wikitech.wikimedia.org/wiki/Performance.wikimedia.org#Coal for more info.

## Overview

### coal

Consumes [EventLogging](https://wikitech.wikimedia.org/wiki/EventLogging) events from Kafka
and produces moving medians to Graphite.

### coal-web

Simple Flask app for serving coal metrics from Graphite.

## Contributing

Code is hosted in [Gerrit](https://gerrit.wikimedia.org/g/performance/coal/), please see https://www.mediawiki.org/wiki/Developer_access for access.

Report issues to [Phabricator](https://phabricator.wikimedia.org/tag/performance-team/).

## Development

Install dependencies and run tests with `tox -v`.

To start coal-web locally, run `.tox/py27/bin/python coal/coal_web.py`
which starts a server at <http://127.0.0.1:5000/>.
