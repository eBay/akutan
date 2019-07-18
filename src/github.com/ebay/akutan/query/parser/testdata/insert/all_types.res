Parsed:
_ #1001 #1002 #1003
_ akutan:qname1 akutan:qname2 akutan:qname3
_ <entity1> <entity2> <entity3>
?var <a> <b> <c>
_ ?var #1002 ?var
_ akutan:qname1 akutan:qname2 true
_ akutan:qname1 akutan:qname2 12
_ akutan:qname1 akutan:qname2 12.230000
_ akutan:qname1 akutan:qname2 "banana"
_ akutan:qname1 akutan:qname2 '2017-01-03' (y-m-d)
_ akutan:qname1 akutan:qname2 true^^isEmpty
_ akutan:qname1 akutan:qname2 12^^inch
_ akutan:qname1 akutan:qname2 12.230000^^inch
_ akutan:qname1 akutan:qname2 "banana"@en_US
_ akutan:qname1 akutan:qname2 '2017-01-03' (y-m-d)^^{0 dateOfBirth}

Parsed Details:
(*parser.Insert)({
  Facts: ([]*parser.Quad) (len=15) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.LiteralID)({
        Value: (uint64) 1001,
        Hint: (string) ""
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 1002,
        Hint: (string) ""
      }),
      Object: (*parser.LiteralID)({
        Value: (uint64) 1003,
        Hint: (string) ""
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname3"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=7) "entity1"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=7) "entity2"
      }),
      Object: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=7) "entity3"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Variable)({
        Name: (string) (len=3) "var"
      }),
      Subject: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "a"
      }),
      Predicate: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "b"
      }),
      Object: (*parser.Entity)({
        ID: (uint64) 0,
        Value: (string) (len=1) "c"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=3) "var"
      }),
      Predicate: (*parser.LiteralID)({
        Value: (uint64) 1002,
        Hint: (string) ""
      }),
      Object: (*parser.Variable)({
        Name: (string) (len=3) "var"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (bool) true
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (int64) 12
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (float64) 12.23
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (string) (len=6) "banana"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) ""
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63618998400,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 3
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralBool)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=7) "isEmpty"
        },
        Value: (bool) true
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralInt)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=4) "inch"
        },
        Value: (int64) 12
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralFloat)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=4) "inch"
        },
        Value: (float64) 12.23
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralString)({
        Language: (parser.Language) {
          ID: (uint64) 0,
          Value: (string) (len=5) "en_US"
        },
        Value: (string) (len=6) "banana"
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname1"
      }),
      Predicate: (*parser.QName)({
        ID: (uint64) 0,
        Value: (string) (len=13) "akutan:qname2"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=11) "dateOfBirth"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63618998400,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 3
      }),
      Specificity: (parser.MatchSpecificity) 0
    })
  }
})
