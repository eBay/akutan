Query:
?s ?p true
?s ?p false
?s ?p true^^<white>

Parsed:
_ ?s ?p true
_ ?s ?p false
_ ?s ?p true^^white

Rewritten:
_ ?s ?p true
_ ?s ?p false
_ ?s ?p true^^white

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=3) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (bool) true
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (bool) false
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=5) "white"
        },
        Value: (bool) true
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
  Where: (parser.WhereClause) (len=3) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (bool) true
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (bool) false
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 10863788102,
          Value: (string) (len=5) "white"
        },
        Value: (bool) true
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

