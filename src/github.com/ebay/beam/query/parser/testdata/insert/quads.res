Parsed:
?fact the:sky beam:is colors:blue
_ ?fact beam:source places:san_francisco
_ ?fact beam:confidence 1.000000

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=3) {
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=4) "fact"
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=7) "the:sky"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=7) "beam:is"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=11) "colors:blue"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "fact"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=11) "beam:source"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=20) "places:san_francisco"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "fact"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=15) "beam:confidence"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 1
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
