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
		{
			name: "basic with limit",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause:   PlainCQLPredicate("1 = 2"),
				Limit:         NullableInt{Valid: true, Int: 123},
			},
			wantStmt: "SELECT fiz, buz FROM foo WHERE 1 = 2 LIMIT 123",
		},
		{
			name: "basic and",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause:   And(Eq(Column("bar")), Eq(Column("fiz"))),
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "SELECT fiz, buz FROM foo WHERE (bar = ?) AND (fiz = ?)",
			wantArgs: []interface{}{3, 1},
		},
	} {
		stc.assert(t)
	}
}
