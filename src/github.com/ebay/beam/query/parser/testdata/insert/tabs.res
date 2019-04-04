Parsed:
_ beam:a beam:b beam:c
?d beam:e beam:f beam:g
_ ?d beam:h beam:i
_ ?d beam:j beam:k
_ beam:l beam:m beam:n
_ beam:o beam:p beam:q

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=6) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:a"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:b"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:c"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=1) "d"
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:e"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:f"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:g"
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
        Value: (string) (len=6) "beam:h"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:i"
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
        Value: (string) (len=6) "beam:j"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:k"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:l"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:m"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:n"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:o"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:p"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=6) "beam:q"
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
