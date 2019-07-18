Query:
?s ?p 1.2e-8
?s ?p 3.14159
?s ?p 3.14159^^xsd:decimal
?s ?p -1.0
?s ?p 0.0
?s ?p -1.2e7

Parsed:
_ ?s ?p 0.000000
_ ?s ?p 3.141590
_ ?s ?p 3.141590^^xsd:decimal
_ ?s ?p -1.000000
_ ?s ?p 0.000000
_ ?s ?p -12000000.000000

Rewritten:
_ ?s ?p 0.000000
_ ?s ?p 3.141590
_ ?s ?p 3.141590^^xsd:decimal
_ ?s ?p -1.000000
_ ?s ?p 0.000000
_ ?s ?p -12000000.000000

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=6) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 1.2e-08
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 3.14159
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=11) "xsd:decimal"
        },
        Value: (float64) 3.14159
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) -1
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 0
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) -1.2e+07
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
  Where: (parser.WhereClause) (len=6) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 1.2e-08
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 3.14159
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 4045557000,
          Value: (string) (len=11) "xsd:decimal"
        },
        Value: (float64) 3.14159
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) -1
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 0
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
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) -1.2e+07
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

