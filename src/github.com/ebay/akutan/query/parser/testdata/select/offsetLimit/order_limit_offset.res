Query:
SELECT ?rims WHERE {
    ?rims rdf:type products:rims
}
ORDERby ?rims
OFFSET 300
LIMIT 100

Parsed:
SELECT ?rims
WHERE {
_ ?rims rdf:type products:rims
}
ORDER BY ASC(?rims)
LIMIT 100 OFFSET 300

Rewritten:
SELECT ?rims
WHERE {
_ ?rims rdf:type products:rims
}
ORDER BY ASC(?rims)
LIMIT 100 OFFSET 300

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=1) {
      (*parser.Variable)({
        Name: (string) (len=4) "rims"
      })
    }
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "rdf:type"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "products:rims"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  },
  Modifiers: (parser.SolutionModifier) {
    OrderBy: ([]parser.OrderCondition) (len=1) {
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=4) "rims"
        })
      }
    },
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(100),
      Offset: (*uint64)(300)
    }
  }
})

Rewritten Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=1) {
      (*parser.Variable)({
        Name: (string) (len=4) "rims"
      })
    }
  },
  Where: (parser.WhereClause) (len=1) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 13542198003,
        Hint: (string) (len=8) "rdf:type"
      }),
      Object: (*parser.LiteralID)({
        Value: (uint64) 8,
        Hint: (string) (len=13) "products:rims"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  },
  Modifiers: (parser.SolutionModifier) {
    OrderBy: ([]parser.OrderCondition) (len=1) {
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=4) "rims"
        })
      }
    },
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(100),
      Offset: (*uint64)(300)
    }
  }
})

