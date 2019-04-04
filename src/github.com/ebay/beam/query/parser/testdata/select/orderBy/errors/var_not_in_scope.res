Query:
SELECT ?rims 
WHERE {
    ?rims rdf:type products:rims
}
ORDER BY ?rim

Parsed:
SELECT ?rims
WHERE {
_ ?rims rdf:type products:rims
}
ORDER BY ASC(?rim)

Rewritten:
Error: invalid query: ORDER BY of ?rim but ?rim not in scope

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=1) {
      (*parser.Variable)({
        Name: (string) (len=4) "rims"
      })
    }
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "rdf:type"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "products:rims"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  },
  Modifiers: (parser.SolutionModifier) {
    OrderBy: ([]parser.OrderCondition) (len=1) {
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=3) "rim"
        })
      }
    },
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(<nil>),
      Offset: (*uint64)(<nil>)
    }
  }
})

