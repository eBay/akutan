Query:
SELECT
    DISTINCT REDUCED ?rim
WHERE {
    ?rim rdf:type products:rims
    ?rim <size> ?sz
}

Parsed:
Error: unable to parse query: line 2 column 14: expected ?
