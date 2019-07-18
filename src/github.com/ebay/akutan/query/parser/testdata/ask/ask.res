Query:
ASK {
	?rims <color> "white"@en
}

Parsed:
ASK 
WHERE {
_ ?rims <color> "white"@en
}

Rewritten:
ASK 
WHERE {
_ ?rims <color> "white"@en
}

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 3,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=5) "color"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) (len=2) "en"
        },
        Value: (string) (len=5) "white"
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
  Type: (parser.QueryType) 3,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 13559708021,
        Hint: (string) (len=7) "<color>"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 728987017,
          Value: (string) (len=2) "en"
        },
        Value: (string) (len=5) "white"
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

