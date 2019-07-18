Parsed:
?zero the:sky akutan:is colors:blue
?one ?zero akutan:source place:san_francisco
?two ?one akutan:source web:weather_com
?three ?two akutan:channel comms:web
_ ?three data:noise 0.650000
_ ?three data:signal 0.800000

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=6) {
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=4) "zero"
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
      ID: (*parser.Variable)({
        Name: (string) (len=3) "one"
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=4) "zero"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:source"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=19) "place:san_francisco"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=3) "two"
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=3) "one"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:source"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=15) "web:weather_com"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=5) "three"
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=3) "two"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=14) "akutan:channel"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=9) "comms:web"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=5) "three"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=10) "data:noise"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 0.65
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=5) "three"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=11) "data:signal"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 0.8
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
