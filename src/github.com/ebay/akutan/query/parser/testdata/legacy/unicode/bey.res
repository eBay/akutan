Query:
?s rdf:type? "Beyoncé"

Parsed:
_ ?s rdf:type? "Beyoncé"

Rewritten:
_ ?s rdf:type? "Beyoncé"

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
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "rdf:type"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (string) (len=8) "Beyoncé"
      }),
      Specificity: (parser.MatchSpecificity) 1
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
  Where: (parser.WhereClause) (len=1) {
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
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (string) (len=8) "Beyoncé"
      }),
      Specificity: (parser.MatchSpecificity) 1
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

