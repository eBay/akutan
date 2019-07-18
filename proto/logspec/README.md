# logspec

Akutan currently (Aug 2018) uses Kafka as its log. We have identified
several problems with Kafka, including some with its protocol, which we've
described in the [Using Apache Kafka in Akutan](../../docs/kakfa.md) document.

This repository contains a specification for a hypothetical log service.

The details of the specification are in the file `log.proto`, which is a gRPC
service definition.

The `github.com/ebay/akutan/blog/logspecclient` package provides a client implementation
that is integrated into Akutan that can use a log that provides this API.
