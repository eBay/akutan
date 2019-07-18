Query:
SELECT
    WHOAMI ?rim
WHERE {
    ?rim rdf:type products:rims
    ?rim <size> ?sz
}

Parsed:
Error: unable to parse query: line 2 column 5: expected ?
