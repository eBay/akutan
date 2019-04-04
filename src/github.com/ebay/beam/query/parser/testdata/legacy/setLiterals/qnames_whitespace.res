Query:
?s rdf:type ?t
?t <in> {products:tires , products:rims , products:lugnuts , products:bolts}

Parsed:
_ ?s rdf:type ?t
_ ?t in {products:tires, products:rims, products:lugnuts, products:bolts}

Rewritten:
_ ?s rdf:type ?t
_ ?t in {products:tires, products:rims, products:lugnuts, products:bolts}

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
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "rdf:type"
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
        Values: ([]parser.Term) (len=4) {
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=14) "products:tires"
          }),
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=13) "products:rims"
          }),
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=16) "products:lugnuts"
          }),
          (*parser.QName)({
            ID: (uint64) 0,
            Value: (string) (len=14) "products:bolts"
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
        Value: (uint64) 13542198003,
        Hint: (string) (len=8) "rdf:type"
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
        Values: ([]parser.Term) (len=4) {
          (*parser.LiteralID)({
            Value: (uint64) 6,
            Hint: (string) (len=14) "products:tires"
          }),
          (*parser.LiteralID)({
            Value: (uint64) 8,
            Hint: (string) (len=13) "products:rims"
          }),
          (*parser.LiteralID)({
            Value: (uint64) 12,
            Hint: (string) (len=16) "products:lugnuts"
          }),
          (*parser.LiteralID)({
            Value: (uint64) 14,
            Hint: (string) (len=14) "products:bolts"
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

