Query:
?x <in> {<Samsung>, #52}
?y <in> {#53, rdf:type}

Parsed:
_ ?x in {<Samsung>, #52}
_ ?y in {#53, rdf:type}

Rewritten:
_ ?x in {<Samsung>, #52}
_ ?y in {#53, rdf:type}

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
        Name: (string) (len=1) "x"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.Entity)({
            ID: (uint64) 0,
            Value: (string) (len=7) "Samsung"
          }),
          (*parser.LiteralID)({
            Value: (uint64) 52,
            Hint: (string) ""
          })
        }
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "y"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.LiteralID)({
            Value: (uint64) 53,
            Hint: (string) ""
          }),
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=8) "rdf:type"
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
        Name: (string) (len=1) "x"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.LiteralID)({
            Value: (uint64) 10574672077,
            Hint: (string) (len=9) "<Samsung>"
          }),
          (*parser.LiteralID)({
            Value: (uint64) 52,
            Hint: (string) ""
          })
        }
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "y"
      }),
      Predicate: (*parser.Operator)({
        Value: (rpc.Operator) 10
      }),
      Object: (*parser.LiteralSet)({
        Values: ([]parser.Term) (len=2) {
          (*parser.LiteralID)({
            Value: (uint64) 53,
            Hint: (string) ""
          }),
          (*parser.LiteralID)({
            Value: (uint64) 13542198003,
            Hint: (string) (len=8) "rdf:type"
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

