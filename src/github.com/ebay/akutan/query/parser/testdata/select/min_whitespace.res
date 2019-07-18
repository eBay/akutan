Query:
select?rims?wheels{
?rims ?wheels "Wheels"
}orderby?rims LIMIT10OFFSET20

Parsed:
SELECT ?rims ?wheels
WHERE {
_ ?rims ?wheels "Wheels"
}
ORDER BY ASC(?rims)
LIMIT 10 OFFSET 20

Rewritten:
Unchanged from parsed version

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
        Name: (string) (len=6) "wheels"
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
      Predicate: (*parser.Variable)({
        Name: (string) (len=6) "wheels"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (string) (len=6) "Wheels"
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
      Limit: (*uint64)(10),
      Offset: (*uint64)(20)
    }
  }
})

