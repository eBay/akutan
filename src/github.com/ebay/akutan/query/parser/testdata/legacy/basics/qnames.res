Query:
rdf:type rdfs:label ?l

Parsed:
_ rdf:type rdfs:label ?l

Rewritten:
_ rdf:type rdfs:label ?l

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
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "rdf:type"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=10) "rdfs:label"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "l"
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
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.LiteralID)({
        Value: (uint64) 13542198003,
        Hint: (string) (len=8) "rdf:type"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 13542198000,
        Hint: (string) (len=10) "rdfs:label"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "l"
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

