Query:
# this is a little nonsensical because you can't mix aggregate and non-aggregate
# items, but the 2nd ?numRims is in scope as it comes from an earlier binding

SELECT (COUNT(?rims) as ?numRims) ?numRims WHERE {
    ?rim rdf:type products:rims
}

Parsed:
SELECT (COUNT(?rims) AS ?numRims) ?numRims
WHERE {
_ ?rim rdf:type products:rims
}

Rewritten:
Error: invalid query: can't mix aggregate and non-aggregate expressions. have (COUNT(?rims) AS ?numRims) and ?numRims

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=2) {
      (*parser.BoundExpression)({
        Expr: (*parser.AggregateExpr)({
          Function: (parser.AggregateFunction) 1,
          Of: (*parser.Variable)({
            Name: (string) (len=4) "rims"
          })
        }),
        As: (*parser.Variable)({
          Name: (string) (len=7) "numRims"
        })
      }),
      (*parser.Variable)({
        Name: (string) (len=7) "numRims"
      })
    }
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=3) "rim"
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
    OrderBy: ([]parser.OrderCondition) <nil>,
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(<nil>),
      Offset: (*uint64)(<nil>)
    }
  }
})

