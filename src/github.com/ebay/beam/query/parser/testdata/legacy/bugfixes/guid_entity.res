Query:
<bob001_guid_f44862d1-3f37-4a31-b8fa-4d9e14edb771> ?p ?o

Parsed:
_ <bob001_guid_f44862d1-3f37-4a31-b8fa-4d9e14edb771> ?p ?o

Rewritten:
Error: invalid query: entity 'bob001_guid_f44862d1-3f37-4a31-b8fa-4d9e14edb771' does not exist

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
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=48) "bob001_guid_f44862d1-3f37-4a31-b8fa-4d9e14edb771"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=1) "o"
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

