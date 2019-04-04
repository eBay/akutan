# Query Language

*This document was written in March 2019 to describe Beam's current query
language.*

## Introduction

A Beam cluster contains a graph of facts, which you can query using Beam's query
language. Queries are based on fact patterns. They ask: do any facts in the
graph satisfy this pattern? The facts in the graph may be static (concrete)
facts physically stored in Beam, or they may be dynamic (inferred) facts that
are generated during query execution.

The query language started as a home grown format, but more recently it has been
transitioning to be much more like
[Sparql](https://en.wikipedia.org/wiki/SPARQL). You can write many useful
queries with the query language as it currently is, but there is still much work
to do. Due to how the query language started and has progressed, there are many
constructs that look similar to RDF or Sparql, but are not exactly the same.
Ideally, it should transition to these standards where possible.

## Fact Building Blocks

Each *fact* consists of a *subject*, *predicate*, *object*, and an *ID*. The
subject and predicate fields must be an *entity*, while the object field may be
either an entity or a *literal value*. Entities in the graph are represented in
the query language by surrounding them in angle brackets (like `<USA>`), by
using a QName (like `rdf:type`), or by using their internal ID (like `#1052`).
Although entities in Beam are similar to RDF, they are not the same as the RDF
equivalents; for example, the prefix of a QName in Beam is not bound to a
namespace URI.

As mentioned above, object fields may contain literal values. Beam supports a
number of strongly typed literals and includes some support for languages and
units:

| Type | Description | Examples |
|---   |---     |---       |
| Entity | an entity in the graph | `<Apple>`, `rdf:type`, `#1052` |
| Int | a 64-bit signed integer with an optional unit | `10`, `20^^<inch>`, `30^^uom:mm` |
| Float | a double-precision floating point with an optional unit | `42.0`, `11.5^^<inch>` |
| Bool | true or false | `true`, `false` |
| String | a unicode string with an optional language | `"Hello"`, `"World"@en` |
| Timestamp | a date/time with amount of precision | `'2019'`, `'2019-03-22'`, `'2017-11-30 14:30:00'` |

The fact's ID allows for encoding information about facts using the *metafacts*
model. This is described more in the [ProtoBeam v3 doc](protobeam_v3.md).


## Basic Query Structure

If you have Beam running, you can try the examples out by first loading the example
data set with `bin/beam-client insert docs/query/example_data.tsv`

Then you can use `beam-client` to run queries, as in the following example:

```bash
$ bin/beam-client query docs/query/fp_01.bql
...
 person        | place       |
 ------------- | ----------- |
 <John_Scalzi> | <Fairfield> |

```

A central piece of the query language is the *fact pattern*. This details one or
more facts that are related to search for. These use one line per fact in the
subject-predicate-object format. A line can also include a leading fact ID field
(to support metafacts). Most fact lines will include one or two variables,
denoted by a leading `?`, in place of concrete fields. Variables allow fact
patterns to match facts with unknown field values.

Fact patterns are usually wrapped in a `SELECT` clause, which indicates which
values from the fact pattern to return. Another option is to use an `ASK`
clause, which is discussed below.

Comments can appear having `# ` (hash followed by a space) at the start of
the line.

Example: Where was the entity with the label "John Scalzi" born?

```
# bin/beam-client query docs/query/fp_01.bql
SELECT * WHERE {
    ?person rdfs:label "John Scalzi"@en
    ?person <born> ?place
}

# result
 person        | place       |
 ------------- | ----------- |
 <John_Scalzi> | <Fairfield> |
```

Example: Of the people born somewhere within California, where was each person
born?

```
# bin/beam-client query docs/query/fp_02.bql
SELECT * WHERE {
    ?person <born> ?place
    ?place <locatedIn> <California>
}

# result
 place       | person        |
 ----------- | ------------- |
 <Fairfield> | <John_Scalzi> |
```

Example: Which Samsung TVs have a size of 65 inches?

```
# bin/beam-client query docs/query/fp_03.bql
SELECT * WHERE {
    ?tv rdf:type <TV>
    ?tv <manufacturer> <Samsung>
    ?tv <size> 65^^<inch>
}

# result
 tv           |
 ------------ |
 <QN65Q6FNAF> |
```

## Comparisions

Values can be compared by specifying additional facts that describe the
comparison. In place of the predicate field, use `<eq>`, `<notEqual>`, `<gt>`,
`<gte>`, `<lt>`, or `<lte>`. Note: this is a different approach than Sparql's
`FILTER` clause.

Example: Which Samsung TVs have a size greater than 36 inches?

```
# bin/beam-client query docs/query/cmp_01.bql
SELECT * WHERE {
    ?tv rdf:type <TV>
    ?tv <manufacturer> <Samsung>
    ?tv <size> ?size
    ?size <gt> 36^^<inch>
}

# result
 tv           | size       |
 ------------ | ---------- |
 <QN65Q6FNAF> | 65^^<inch> |
 <NU7100>     | 40^^<inch> |
```

Example: Is the UK in the European Union as of March 22, 2019?

This query uses metafacts. `ASK` queries are described more later.

```
# bin/beam-client query docs/query/cmp_02.bql
ASK WHERE {
    ?fact <UK> <memberOf> <EuropeanUnion>
        ?fact <validUntil> ?until
        ?fact <validFrom> ?from
        ?until <gte> '2019-03-22'
        ?from <lte> '2019-03-22'
}

# result
 result |
 ------ |
 true   |
```

## Transitive Predicates

Beam currently assumes that predicates are transitive. For example, if
`<a> <type> <b>` and `<b> <type> <c>`, then by the transitive nature of
`<type>`, it's also true that `<a> <type> <c>`. Beam will generate these
inferred facts at query time.

Of course, there are predicates that are not logically transitive. For example,
from `<a> <trusts> <b>` and `<b> <trusts> <c>`, one should not infer that
`<a> <trusts> <c>`. For now, one should be careful when modeling such graphs
with Beam.


Example: Of the people born somewhere within the USA, where was each person
born?

```
# bin/beam-client query docs/query/tp_01.bql
SELECT * WHERE {
    ?person <born> ?place
    ?place <locatedIn> <USA>
}

# result
 place         | person            |
 ------------- | ----------------- |
 <Fairfield>   | <John_Scalzi>     |
 <Conway>      | <William_Gibson>  |
 <Forte_Meade> | <Neal_Stephenson> |
```

Note that the example data does not include any fact that says
`<Fairfield> <locatedIn> <USA>`. Beam inferred this fact from:
```
<Fairfield>  <locatedIn> <California>
<California> <locatedIn> <USA>
```

## Set Literals

Fact patterns can also be used to express a set of possible values. This is done using the
`<in>` predicate and a comma-separated list of values within braces as an object.

Example: Who was born somewhere within Maryland or South Carolina?

```
# bin/beam-client query docs/query/set_01.bql
SELECT ?person WHERE {
    ?person <born> ?loc
    ?loc <locatedIn> ?place
    ?place <in> { <Maryland> , <South_Carolina> }
}

# result
 person            |
 ----------------- |
 <William_Gibson>  |
 <Neal_Stephenson> |
```

## Optional Match

A Trailing `?` on the predicate can be used to indicate that a fact is optional.
If the fact exists, it'll be included in the result. If it doesn't, the rest of
fact pattern will still produce a result. Variables from the optional match will
be nil if the fact is not matched.

Example: Who was born within the USA, and if we know, where are they now?

```
# bin/beam-client query docs/query/opt_01.bql
SELECT ?person ?bornWhere ?livesWhere WHERE {
    ?person <born> ?bornWhere
    ?bornWhere <locatedIn> <USA>
    ?person <lives>? ?livesWhere
}

# results
 person            | bornWhere     | livesWhere |
 ----------------- | ------------- | ---------- |
 <John_Scalzi>     | <Fairfield>   | <Bradford> |
 <William_Gibson>  | <Conway>      | (nil)      |
 <Neal_Stephenson> | <Forte_Meade> | (nil)      |
```

In the example, if there's no `<lives>` fact for a person, then the variable
`?livesWhere` is nil for that person.

## Order By

Select queries can optionally specify an `ORDER BY` clause to sort the results.
The clause should list the variable(s) to sort on. By default values are sorted
in ascending order, but `ASC(?var)` and `DESC(?var)` keywords can be used to
explicitly indicate the order.

Example: List Samsung TVs from largest to smallest.

```
# bin/beam-client query docs/query/order_01.bql
SELECT ?tv ?size WHERE {
    ?tv rdf:type <TV>
    ?tv <manufacturer> <Samsung>
    ?tv <size> ?size
}
ORDER BY DESC(?size)

# result
 tv           | size       |
 ------------ | ---------- |
 <QN65Q6FNAF> | 65^^<inch> |
 <NU7100>     | 40^^<inch> |
 <N5300>      | 32^^<inch> |
```

## Limit and Offset

Select queries can also specify `LIMIT` and/or `OFFSET` clauses. Limit caps the
returned number of results to the indicated value. Offset indicates how many
results to skip before returning them. These are typically used to implement
paging and are almost always used along with `ORDER BY`.

Example: Which is the largest Samsung TV?

```
# bin/beam-client query docs/query/limit_01.bql
SELECT ?tv ?size WHERE {
    ?tv rdf:type <TV>
    ?tv <manufacturer> <Samsung>
    ?tv <size> ?size
}
ORDER BY DESC(?size)
LIMIT 1

# result
 tv           | size       |
 ------------ | ---------- |
 <QN65Q6FNAF> | 65^^<inch> |
 ```

Example: Which is the second-largest Samsung TV?

```
# bin/beam-client query docs/query/limit_02.bql
SELECT ?tv ?size WHERE {
    ?tv rdf:type <TV>
    ?tv <manufacturer> <Samsung>
    ?tv <size> ?size
}
ORDER BY DESC(?size)
LIMIT 1
OFFSET 1

# result
 tv       | size       |
 -------- | ---------- |
 <NU7100> | 40^^<inch> |
 ```

## Aggregate functions

Beam currently supports one aggregate function, `COUNT`, which returns the
number of results.

Example: How many people were born somewhere in the USA?

```
# bin/beam-client query docs/query/count_01.bql
SELECT (COUNT(?person) as ?num) WHERE {
    ?person <born> ?place
    ?place <locatedIn> <USA>
}

# result
 num |
 --- |
 3   |
```

## Distinct

Select can also return only distinct results, via `SELECT DISTINCT`.

Example: For each TV, who makes it? (without distinct)

```
# bin/beam-client query docs/query/d_01.bql
SELECT ?manf WHERE {
    ?p rdf:type <TV>
    ?p <manufacturer> ?manf
}
 manf      |
 --------- |
 <Samsung> |
 <Samsung> |
 <Samsung> |
```

Example: What are the companies that manufacture TVs?

```
# bin/beam-client query docs/query/d_02.bql
SELECT DISTINCT ?manf WHERE {
    ?p rdf:type <TV>
    ?p <manufacturer> ?manf
}

# result
 manf      |
 --------- |
 <Samsung> |
 ```

## Ask

Instead of `SELECT`, you can use the `ASK` query keyword. This returns true
or false depending on if the `WHERE` clause found any results.

Example: Was anyone born in Maryland?

```
# bin/beam-client query docs/query/ask_01.bql
ASK WHERE {
    ?person <born> ?place
    ?place <locatedIn> <Maryland>
}

# result
 result |
 ------ |
 true   |
```

## Query Execution

To programmatically run your query, you submit the query string to the `query`
RPC in the API servers' gRPC service. For more details, see the
`protos/api/beam_api.proto` file. The response from query is a stream of 1 or
more chunks of results. The `beam-client query` operation used in the examples
is a command line tool that wraps this gRPC API.

For details of how the query engine executes the queries, see the
[ProtoBeam v3](protobeam_v3.md) doc.
