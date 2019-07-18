Parsed:
_ the:sky akutan:is colors:blue
_ <iPhone> <color> <blue>
_ phones:Pixel3 props:size 64^^units:gigabytes

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=3) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=7) "the:sky"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=9) "akutan:is"
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
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=6) "iPhone"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=5) "color"
      }),
      Object: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=4) "blue"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "phones:Pixel3"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=10) "props:size"
      }),
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=15) "units:gigabytes"
        },
        Value: (int64) 64
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
