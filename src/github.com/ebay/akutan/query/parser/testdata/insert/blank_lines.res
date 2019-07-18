Parsed:
_ <s> <p> <o1>
_ <s> <p> <o2>

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=2) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "s"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "p"
      }),
      Object: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=2) "o1"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "s"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "p"
      }),
      Object: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=2) "o2"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
