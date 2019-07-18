Query:
?s <brand> ?b
			 ?b <in> {"Samsung"@en,"Samsung_(Complex)"@en}

Parsed:
_ ?s <brand> ?b
_ ?b in {"Samsung"@en, "Samsung_(Complex)"@en}

Rewritten:
_ ?s <brand> ?b
_ ?b in {"Samsung"@en, "Samsung_(Complex)"@en}

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
        Value: (string) (len=5) "brand"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "b"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "b"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.LiteralString)({
            Language: (parser.Language) {
              ID: (uint64) 0,
              Value: (string) (len=2) "en"
            },
            Value: (string) (len=7) "Samsung"
          }),
          (*parser.LiteralString)({
            Language: (parser.Language) {
              ID: (uint64) 0,
              Value: (string) (len=2) "en"
            },
            Value: (string) (len=17) "Samsung_(Complex)"
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
        Value: (uint64) 2,
        Hint: (string) (len=7) "<brand>"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "b"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "b"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.LiteralString)({
            Language: (parser.Language) {
              ID: (uint64) 728987017,
              Value: (string) (len=2) "en"
            },
            Value: (string) (len=7) "Samsung"
          }),
          (*parser.LiteralString)({
            Language: (parser.Language) {
              ID: (uint64) 728987017,
              Value: (string) (len=2) "en"
            },
            Value: (string) (len=17) "Samsung_(Complex)"
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

