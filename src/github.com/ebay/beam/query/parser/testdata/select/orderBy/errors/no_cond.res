Query:
SELECT ?rims
WHERE {
    ?rims rdf:type products:rims
}
ORDER BY
LIMIT 10

Parsed:
Error: unable to parse query: line 6 column 1: expected DESC
