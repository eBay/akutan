This directory contains point-in-time snapshots of what we thought about a
certain topic at a certain time. While many of these documents build on each
other, it should be possible to read these in isolation and to approach them in
any order. We suggest reviewing this chronological list first, then jumping
around based on your interests.

- [ProtoAkutan v1](protoakutan_v1.md) (November 2017): This document introduces
  Akutan's basic architecture, which is based around a central log. It describes
  the expected benefits and challenges of a central log, as well as Akutan's
  approach to distributed transactions.

- [ProtoAkutan v2](protoakutan_v2.md) (March 2018): This document summarized the
  state of the system back then as a property graph store. It also includes some
  discussion of storing data in RocksDB compared to other embedded key-value
  stores.

- [Booting Akutan Views](booting_views.md) (March 2018): Part of the
  ProtoAkutan v2 series, this document describes how view servers in Akutan start up
  once the oldest part of the log has been discarded.

- [Using Apache Kafka in Akutan](kafka.md) (March 2018): Part of the ProtoAkutan v2
  series, this document describes challenges with using Kafka as Akutan's log, and
  why it might not be a great fit.

- [ProtoAkutan v3](protoakutan_v3.md) (August 2018): This document summarized the
  state of the system back then, after adapting it from a property graph store
  to a knowledge graph store. It describes the overall system components (which
  are still accurate) and the query language at that time (which has evolved
  significantly since; see the Query doc below). It includes details on the
  query processor, including the parser, cost-based query planner, parallel
  query execution engine, and RPC fanout mechanism.

- [Control Plane Requirements](control_plane.md) (September 2018): This document
  explores the various scenarios that a control plane (or manual operator) will
  need to handle for a cross-datacenter production Akutan deployment.

- [Building Feature-Rich Data Stores Using a Central Log](central_log_arch.md)
  (January 2019): This short document argues that a central log forms the
  basis of an architecture that is well-suited for systems like Akutan.

- [High Availability and Disaster Recovery](high_availability.md) (March 2019):
  This short document explains how fault-tolerance is built into Akutan's
  architecture.

- [Update RFC](rfc_update.md) (March 2019): This long document proposes a new
  mechanism for inserting, deleting, and updating facts in Akutan, based on the
  SPARQL Update specification. Only part of this RFC has been implemented at
  this time.

- [Query](query.md) (March 2019): This document describes Akutan's current query
  language, which is a cross between SPARQL and the version described in the
  ProtoAkutan v3 doc.
