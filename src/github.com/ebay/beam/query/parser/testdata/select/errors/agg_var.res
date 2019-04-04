Query:
SELECT (COUNT(?rims) AS ?rimCount) ?brand
WHERE {
    ?rims <color> "Red"
    ?rims <brand> ?brand
}

Parsed:
SELECT (COUNT(?rims) AS ?rimCount) ?brand
WHERE {
_ ?rims <color> "Red"
_ ?rims <brand> ?brand
}

Rewritten:
Error: invalid query: can't mix aggregate and non-aggregate expressions. have (COUNT(?rims) AS ?rimCount) and ?brand

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
          Name: (string) (len=8) "rimCount"
        })
      }),
      (*parser.Variable)({
        Name: (string) (len=5) "brand"
      })
    }
  },
  Where: (parser.WhereClause) (len=2) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=5) "color"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (string) (len=3) "Red"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=5) "brand"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=5) "brand"
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

