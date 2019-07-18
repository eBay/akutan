Query:
?x <lt> {0.5}

Parsed:
_ ?x < {0.500000}

Rewritten:
Error: literal sets must use operator 'in' (found '<')

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "x"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 1
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=1) {
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (float64) 0.5
          })
        }
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

