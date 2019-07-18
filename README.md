# Akutan

[![Build Status](https://travis-ci.com/eBay/akutan.svg?branch=master)](https://travis-ci.com/eBay/akutan)
[![GoDoc](https://godoc.org/github.com/ebay/akutan/src/github.com/ebay/akutan?status.svg)](https://godoc.org/github.com/ebay/akutan/src/github.com/ebay/akutan)

There's a blog post that's a [good introduction to Akutan](https://www.ebayinc.com/stories/blogs/tech/beam-a-distributed-knowledge-graph-store/).

Akutan is a distributed knowledge graph store, sometimes called an RDF store or a
triple store. Knowledge graphs are suitable for modeling data that is highly
interconnected by many types of relationships, like encyclopedic information
about the world. A knowledge graph store enables rich queries on its data, which
can be used to power real-time interfaces, to complement machine learning
applications, and to make sense of new, unstructured information in the context
of the existing knowledge.

How to model your data as a knowledge graph and how to query it will feel a bit
different for people coming from SQL, NoSQL, and property graph stores. In a
knowledge graph, data is represented as a single table of *facts*, where each
fact has a *subject*, *predicate*, and *object*. This representation enables the
store to sift through the data for complex queries and to apply inference rules
that raise the level of abstraction. Here's an example of a tiny graph:

subject         | predicate | object
----------------|-----------|-----------------
`<John_Scalzi>` | `<born>`  | `<Fairfield>`
`<John_Scalzi>` | `<lives>` | `<Bradford>`
`<John_Scalzi>` | `<wrote>` | `<Old_Mans_War>`

To learn about how to represent and query data in Akutan, see
[docs/query.md](docs/query.md).

Akutan is designed to store large graphs that cannot fit on a single server. It's
scalable in how much data it can store and the rate of queries it can execute.
However, Akutan serializes all changes to the graph through a central log, which
fundamentally limits the total rate of change. The rate of change won't improve
with a larger number of servers, but a typical deployment should be able to
handle tens of thousands of changes per second. In exchange for this limitation,
Akutan's architecture is a relatively simple one that enables many features. For
example, Akutan supports transactional updates and historical global snapshots. We
believe this trade-off is suitable for most knowledge graph use cases, which
accumulate large amounts of data but do so at a modest pace. To learn more about
Akutan's architecture and this trade-off, see
[docs/central_log_arch.md](docs/central_log_arch.md).

Akutan isn't ready for production-critical deployments, but it's useful today for
some use cases. We've run a 20-server deployment of Akutan for development
purposes and off-line use cases for about a year, which we've most commonly
loaded with a dataset of about 2.5 billion facts. We believe Akutan's current
capabilities exceed this capacity and scale; we haven't yet pushed Akutan to its
limits. The project has a good architectural foundation on which additional
features can be built and higher performance could be achieved.

Akutan needs more love before it can be used for production-critical deployments.
Much of Akutan's code consists of high-quality, documented, unit-tested modules,
but some areas of the code base are inherited from Akutan's earlier prototype days
and still need attention. In other places, some functionality is lacking before
Akutan could be used as a critical production data store, including deletion of
facts, backup/restore, and automated cluster management. We have filed
GitHub issues for these and a few other things. There are also areas where Akutan
could be improved that wouldn't necessarily block production usage. For example,
Akutan's query language is not quite compatible with Sparql, and its inference
engine is limited.

So, Akutan has a nice foundation and may be useful to some people, but it also
needs additional love. If that's not for you, here are a few alternative
open-source knowledge and property graph stores that you may want to consider
(we have no affiliation with these projects):

- [Blazegraph](https://github.com/blazegraph/database): an RDF store. Supports
  several query languages, including SPARQL and Gremlin. Disk-based,
  single-master, scales out for reads only. Seems unmaintained. Powers
  <https://query.wikidata.org/>.
- [Dgraph](https://github.com/dgraph-io/dgraph): a triple-oriented property
  graph store. GraphQL-like query language, no support for SPARQL. Disk-based,
  scales out.
- [Neo4j](https://github.com/neo4j/neo4j): a property graph store. Cypher query
  language, no support for SPARQL. Single-master, scales out for reads only.
- See also Wikipedia's
  [Comparison of Triplestores](https://en.wikipedia.org/wiki/Comparison_of_triplestores)
  page.

The remainder of this README describes how to get Akutan up and running. Several
documents under the `docs/` directory describe aspects of Akutan in more
detail; see [docs/README.md](docs/README.md) for an overview.

## Installing dependencies and building Akutan

Akutan has the following system dependencies:
 - It's written in [Go](https://golang.org/). You'll need v1.11.5 or newer.
 - Akutan uses [Protocol Buffers](https://developers.google.com/protocol-buffers/)
   extensively to encode messages for [gRPC](https://grpc.io/), the log of data
   changes, and storage on disk. You'll need protobuf version 3. We reccomend
   3.5.2 or later. Note that 3.0.x is the default in many Linux distributions, but
   doesn't work with the Akutan build.
 - Akutan's Disk Views store their facts in [RocksDB](https://rocksdb.org/).

On Mac OS X, these can all be installed via [Homebrew](https://brew.sh/):

	$ brew install golang protobuf rocksdb zstd

On Ubuntu, refer to the files within the [docker/](docker/) directory for
package names to use with `apt-get`.

After cloning the Akutan repository, pull down several Go libraries and additional
Go tools:

	$ make get

Finally, build the project:

	$ make build

## Running Akutan locally

The fastest way to run Akutan locally is to launch the in-memory log store:

	$ bin/plank

Then open another terminal and run:

	$ make run

This will bring up several Akutan servers locally. It starts an API server that
listens on localhost for gRPC requests on port 9987 and for HTTP requests on
port 9988, such as <http://localhost:9988/stats.txt>.

The easiest way to interact with the API server is using `bin/akutan-client`. See
[docs/query.md](docs/query.md) for examples. The API server exposes the
`FactStore` gRPC service defined in
[proto/api/akutan_api.proto](proto/api/akutan_api.proto).

## Deployment concerns

### The log

Earlier, we used `bin/plank` as a log store, but this is unsuitable for real
usage! Plank is in-memory only, isn't replicated, and by default, it only
keeps 1000 entries at a time. It's only meant for development.

Akutan also supports using [Apache Kafka](https://kafka.apache.org/) as its log
store. This is recommended over Plank for any deployment. To use Kafka, follow the
[Kafka quick start](https://kafka.apache.org/quickstart) guide to install
Kafka, start ZooKeeper, and start Kafka. Then create a topic called "akutan"
(not "test" as in the Kafka guide) with `partitions` set to 1. You'll want to
configure Kafka to synchronously write entries to disk.

To use Kafka with Akutan, set the `akutanLog`'s `type` to `kafka` in your Akutan
configuration (default: `local/config.json`), and update the `locator`'s
`addresses` accordingly (Kafka uses port 9092 by default). You'll need to clear
out Akutan's Disk Views' data before restarting the cluster. The Disk Views
by default store their data in $TMPDIR/rocksdb-akutan-diskview-{space}-{partition}
so you can delete them all with `rm -rf $TMPDIR/rocksdb-akutan-diskview*`

### Docker and Kubernetes

This repository includes support for running Akutan inside
[Docker](https://www.docker.com/) and
[Minikube](https://kubernetes.io/docs/setup/minikube/). These environments can
be tedious for development purposes, but they're useful as a step towards a
modern and robust production deployment.

See `cluster/k8s/Minikube.md` file for the steps to build and deploy Akutan
services in `Minikube`. It also includes the steps to build the Docker images.

### Distributed tracing

Akutan generates distributed [OpenTracing](https://opentracing.io/) traces for use
with [Jaeger](https://www.jaegertracing.io/). To try it, follow the
[Jaeger Getting Started Guide](https://www.jaegertracing.io/docs/getting-started/#all-in-one-docker-image)
for running the all-in-one Docker image. The default `make run` is configured to
send traces there, which you can query at <http://localhost:16686>. The Minikube
cluster also includes a Jaeger all-in-one instance.

## Development

### VS Code

You can use whichever editor you'd like, but this repository contains some
configuration for [VS Code](https://code.visualstudio.com/Download). We
suggest the following extensions:
 - [Go](https://marketplace.visualstudio.com/items?itemName=ms-vscode.Go)
 - [Code Spell Checker](https://marketplace.visualstudio.com/items?itemName=streetsidesoftware.code-spell-checker)
 - [Rewrap](https://marketplace.visualstudio.com/items?itemName=stkb.rewrap)
 - [vscode-proto3](https://marketplace.visualstudio.com/items?itemName=zxh404.vscode-proto3)
 - [Docker](https://marketplace.visualstudio.com/items?itemName=PeterJausovec.vscode-docker)

Override the default settings in `.vscode/settings.json` with
[./vscode-settings.json5](./vscode-settings.json5).

### Test targets

The `Makefile` contains various targets related to running tests:

Target       | Description
------------ | -----------
`make test`  | run all the akutan unit tests
`make cover` | run all the akutan unit tests and open the web-based coverage viewer
`make lint`  | run basic code linting
`make vet`   | run all static analysis tests including linting and formatting

## License Information

Copyright 2019 eBay Inc.

Primary authors: Simon Fell, Diego Ongaro, Raymond Kroeker, Sathish Kandasamy

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at <https://www.apache.org/licenses/LICENSE-2.0>.

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.


----
**Note** the project was renamed to Akutan in July 2019.
