package cqlbuilder

import "testing"

func TestDeleteStatement(t *testing.T) {
	for _, stc := range []statementTestCase{
		{
			name: "basic",
			stmt: DeleteStatement{
				Table:       "foo",
				WhereClause: Eq(Column("bar")),
			},
			vs:       map[string]interface{}{"bar": 3},
			wantStmt: "DELETE FROM foo WHERE bar = ?",
			wantArgs: []interface{}{3},
		},
		{
			name: "lwt field",
			stmt: DeleteStatement{
				Table:       "foo",
				Fields:      []Marker{Column("fiz")},
				WhereClause: Eq(Column("bar")),
				LWTClause:   PredicateLWTClause{Predicate: Eq(Column("buz"))},
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "DELETE fiz FROM foo WHERE bar = ? IF buz = ?",
			wantArgs: []interface{}{3, 2},
		},
	} {
		stc.assert(t)
	}
}
