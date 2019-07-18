Query:
?s ?p -42
?s ?p -42^^xsd:integer
?s ?p 42
?s ?p 0

Parsed:
_ ?s ?p -42
_ ?s ?p -42^^xsd:integer
_ ?s ?p 42
_ ?s ?p 0

Rewritten:
_ ?s ?p -42
_ ?s ?p -42^^xsd:integer
_ ?s ?p 42
_ ?s ?p 0

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=4) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) -42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=11) "xsd:integer"
        },
        Value: (int64) -42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) 42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) 0
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
  Where: (parser.WhereClause) (len=4) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) -42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 4045560000,
          Value: (string) (len=11) "xsd:integer"
        },
        Value: (int64) -42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) 42
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
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) 0
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

