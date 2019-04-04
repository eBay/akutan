Query:
?s <size> ?t
?t <in> {0.5,1.0,products:tires}

Parsed:
_ ?s <size> ?t
_ ?t in {0.500000, 1.000000, products:tires}

Rewritten:
Error: literal set values must be of the same type, '0.500000' and 'products:tires' differ

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=2) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=4) "size"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "t"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "t"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=3) {
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (float64) 0.5
          }),
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (float64) 1
          }),
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=14) "products:tires"
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

