Query:
SELECT * (COUNT(?rims) AS ?rimCount) 
WHERE {
    ?rims <color> "Red"
}

Parsed:
Error: unable to parse query: line 1 column 10: expected WHERE
