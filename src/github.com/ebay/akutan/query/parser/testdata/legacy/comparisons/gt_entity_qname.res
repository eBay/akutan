Query:
?s <gt> products:vehicle

Parsed:
_ ?s > products:vehicle

Rewritten:
Error: invalid query: only <eq> and <notEqual> are allowed comparisons when the right side is an entity

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
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 3
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=16) "products:vehicle"
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

