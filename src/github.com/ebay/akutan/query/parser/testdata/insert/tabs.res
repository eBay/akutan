Parsed:
_ akutan:a akutan:b akutan:c
?d akutan:e akutan:f akutan:g
_ ?d akutan:h akutan:i
_ ?d akutan:j akutan:k
_ akutan:l akutan:m akutan:n
_ akutan:o akutan:p akutan:q

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=6) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:a"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:b"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:c"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=1) "d"
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:e"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:f"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:g"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "d"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:h"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:i"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "d"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:j"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:k"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:l"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:m"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:n"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:o"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:p"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=8) "akutan:q"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
