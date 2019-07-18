Query:
SELECT ?rims WHERE {
    ?rims rdf:type products:rims
}
LIMIT 10
ORDER BY ASC(?rims)

Parsed:
Error: unable to parse query: line 5 column 1: unparsed text: 'ORDER BY ASC(?rims)'
