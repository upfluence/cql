package cqlbuilder

import "testing"

func TestSelectStatement(t *testing.T) {
	for _, stc := range []statementTestCase{
		{
			name: "basic",
			stmt: SelectStatement{
				Table:          "foo",
				SelectClauses:  []Marker{Column("fiz"), Column("buz")},
				WhereClause:    Eq(Column("bar")),
				AllowFiltering: true,
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "SELECT fiz, buz FROM foo WHERE bar = ? ALLOW FILTERING",
			wantArgs: []interface{}{3},
		},
	} {
		stc.assert(t)
	}
}
