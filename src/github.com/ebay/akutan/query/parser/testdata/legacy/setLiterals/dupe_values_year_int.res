Query:
?s <size> ?t
?t <in> {  1990, 2018, '2018' }
# 2018 is an int and '2018' is a year. They shouldn't be considered
# duplicates or the same type.

Parsed:
_ ?s <size> ?t
_ ?t in {1990, 2018, '2018' (y)}

Rewritten:
Error: literal set values must be of the same type, '1990' and ''2018' (y)' differ

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
          (*parser.LiteralInt)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (int64) 1990
          }),
          (*parser.LiteralInt)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (int64) 2018
          }),
          (*parser.LiteralTime)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) ""
            },
            Value: (time.Time) {
              wall: (uint64) 0,
              ext: (int64) 63650361600,
              loc: (*time.Location)(<nil>)
            },
            Precision: (api.Precision) 1
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

