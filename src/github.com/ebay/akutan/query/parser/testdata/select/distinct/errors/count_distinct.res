Query:
SELECT 
 (count(?rim) AS ?rims) DISTINCT
WHERE {
    ?rim rdf:type products:rims
    ?rim <size> ?sz
}

Parsed:
Error: unable to parse query: line 2 column 25: expected WHERE
