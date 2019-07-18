Query:
?s <size> ?t
?t <in> {0.5^^<m>,1.0^^<m>,1.5^^<m>}

Parsed:
_ ?s <size> ?t
_ ?t in {0.500000^^m, 1.000000^^m, 1.500000^^m}

Rewritten:
_ ?s <size> ?t
_ ?t in {0.500000^^m, 1.000000^^m, 1.500000^^m}

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
              Value: (string) (len=1) "m"
            },
            Value: (float64) 0.5
          }),
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) (len=1) "m"
            },
            Value: (float64) 1
          }),
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 0,
              Value: (string) (len=1) "m"
            },
            Value: (float64) 1.5
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

Rewritten Details:
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
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 10,
        Hint: (string) (len=6) "<size>"
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
              ID: (uint64) 16,
              Value: (string) (len=1) "m"
            },
            Value: (float64) 0.5
          }),
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 16,
              Value: (string) (len=1) "m"
            },
            Value: (float64) 1
          }),
          (*parser.LiteralFloat)({
            Unit: (parser.Unit) {
              ID: (uint64) 16,
              Value: (string) (len=1) "m"
            },
            Value: (float64) 1.5
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

