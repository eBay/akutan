Query:
?s ?p '2018'^^xsd:date
?s ?p '2018-07'^^xsd:date
?s ?p '2018-07-16'^^xsd:date
?s ?p '2018-07-16 17'^^xsd:date
?s ?p '2018-07-16 17:09'^^xsd:date
?s ?p '2018-07-16 17:09:15'^^xsd:date
?s ?p '2018-07-16 17:09:15.1'^^xsd:date
?s ?p '2018-07-16 17:09:15.999999'^^xsd:date

Parsed:
_ ?s ?p '2018' (y)^^{0 xsd:date}
_ ?s ?p '2018-07' (y-m)^^{0 xsd:date}
_ ?s ?p '2018-07-16' (y-m-d)^^{0 xsd:date}
_ ?s ?p '2018-07-16T17' (y-m-d h)^^{0 xsd:date}
_ ?s ?p '2018-07-16T17:09' (y-m-d h:m)^^{0 xsd:date}
_ ?s ?p '2018-07-16T17:09:15' (y-m-d h:m:s)^^{0 xsd:date}
_ ?s ?p '2018-07-16T17:09:15.000000001' (y-m-d h:m:s.n)^^{0 xsd:date}
_ ?s ?p '2018-07-16T17:09:15.000999999' (y-m-d h:m:s.n)^^{0 xsd:date}

Rewritten:
_ ?s ?p '2018' (y)^^{809287000 xsd:date}
_ ?s ?p '2018-07' (y-m)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16' (y-m-d)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16T17' (y-m-d h)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16T17:09' (y-m-d h:m)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16T17:09:15' (y-m-d h:m:s)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16T17:09:15.000000001' (y-m-d h:m:s.n)^^{809287000 xsd:date}
_ ?s ?p '2018-07-16T17:09:15.000999999' (y-m-d h:m:s.n)^^{809287000 xsd:date}

Parsed Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=8) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63650361600,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 1
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63666000000,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 2
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667296000,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 3
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357200,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 4
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357740,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 5
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 6
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 1,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 7
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 0,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 999999,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 7
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

Rewritten Details:
(*parser.Query)({
  Type: (parser.QueryType) 1,
  Select: (parser.SelectClause) {
    Keyword: (parser.selectClauseKeyword) <nil>,
    Items: ([]parser.selectClauseItem) <nil>
  },
  Where: (parser.WhereClause) (len=8) {
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63650361600,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 1
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63666000000,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 2
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667296000,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 3
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357200,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 4
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357740,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 5
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 0,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 6
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 1,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 7
      }),
      Specificity: (parser.MatchSpecificity) 0
    }),
    (*parser.Quad)({
      ID: (*parser.Nil)({
      }),
      Subject: (*parser.Variable)({
        Name: (string) (len=1) "s"
      }),
      Predicate: (*parser.Variable)({
        Name: (string) (len=1) "p"
      }),
      Object: (*parser.LiteralTime)({
        Unit: (parser.Unit) {
          ID: (uint64) 809287000,
          Value: (string) (len=8) "xsd:date"
        },
        Value: (time.Time) {
          wall: (uint64) 999999,
          ext: (int64) 63667357755,
          loc: (*time.Location)(<nil>)
        },
        Precision: (api.Precision) 7
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

