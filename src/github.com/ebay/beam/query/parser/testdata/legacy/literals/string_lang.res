Query:
#1 <Samsung> rdfs:label "Samsung Group"@en

Parsed:
#1 <Samsung> rdfs:label "Samsung Group"@en

Rewritten:
#1 <Samsung> rdfs:label "Samsung Group"@en

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.LiteralID)({
        Value: (uint64) 1,
        Hint: (string) ""
      }),
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=7) "Samsung"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=10) "rdfs:label"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) (len=2) "en"
        },
        Value: (string) (len=13) "Samsung Group"
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
      ID: (*parser.LiteralID)({
        Value: (uint64) 1,
        Hint: (string) ""
      }),
      Subject: (*parser.LiteralID)({
        Value: (uint64) 10574672077,
        Hint: (string) (len=9) "<Samsung>"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 13542198000,
        Hint: (string) (len=10) "rdfs:label"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 728987017,
          Value: (string) (len=2) "en"
        },
        Value: (string) (len=13) "Samsung Group"
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

