Query:
select ?rim, ?size
where {
    ?rim rdf:type products:rims
    ?rim <size> ?size
}

Parsed:
Error: unable to parse query: line 1 column 12: expected WHERE
