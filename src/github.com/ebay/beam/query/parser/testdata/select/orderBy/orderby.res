Query:
SELECT ?rims?sz
WHERE {
    ?rims rdf:type products:rims
    ?rims <size> ?sz
    ?rims <color> ?color
}
ORDER BY DESC(?sz) ?rims ASC(?color)

Parsed:
SELECT ?rims ?sz
WHERE {
_ ?rims rdf:type products:rims
_ ?rims <size> ?sz
_ ?rims <color> ?color
}
ORDER BY DESC(?sz) ASC(?rims) ASC(?color)

Rewritten:
SELECT ?rims ?sz
WHERE {
_ ?rims rdf:type products:rims
_ ?rims <size> ?sz
_ ?rims <color> ?color
}
ORDER BY DESC(?sz) ASC(?rims) ASC(?color)

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=2) {
      (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      (*parser.Variable)({
        Name: (string) (len=2) "sz"
      })
    }
  },
  Where: (parser.WhereClause) (len=3) {
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
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=4) "size"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=2) "sz"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
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
      Object: (*parser.Variable)({
        Name: (string) (len=5) "color"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  },
  Modifiers: (parser.SolutionModifier) {
    OrderBy: ([]parser.OrderCondition) (len=3) {
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 2,
        On: (*parser.Variable)({
          Name: (string) (len=2) "sz"
        })
      },
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=4) "rims"
        })
      },
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=5) "color"
        })
      }
    },
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(<nil>),
      Offset: (*uint64)(<nil>)
    }
  }
})

Rewritten Details:
(*parser.Query)({
  Type: (parser.QueryType) 2,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) (len=2) {
      (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      (*parser.Variable)({
        Name: (string) (len=2) "sz"
      })
    }
  },
  Where: (parser.WhereClause) (len=3) {
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
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "rims"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 10,
        Hint: (string) (len=6) "<size>"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=2) "sz"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
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
      Object: (*parser.Variable)({
        Name: (string) (len=5) "color"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  },
  Modifiers: (parser.SolutionModifier) {
    OrderBy: ([]parser.OrderCondition) (len=3) {
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 2,
        On: (*parser.Variable)({
          Name: (string) (len=2) "sz"
        })
      },
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=4) "rims"
        })
      },
      (parser.OrderCondition) {
        Direction: (parser.SortDirection) 1,
        On: (*parser.Variable)({
          Name: (string) (len=5) "color"
        })
      }
    },
    Paging: (parser.LimitOffset) {
      Limit: (*uint64)(<nil>),
      Offset: (*uint64)(<nil>)
    }
  }
})

