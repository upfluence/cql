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
			vs:       map[string]any{"bar": 3},
			wantStmt: "DELETE FROM foo WHERE bar = ?",
			wantArgs: []any{3},
		},
		{
			name: "lwt field",
			stmt: DeleteStatement{
				Table:       "foo",
				Fields:      []Marker{Column("fiz")},
				WhereClause: Eq(Column("bar")),
				LWTClause:   PredicateLWTClause{Predicate: Eq(Column("buz"))},
			},
			vs:       map[string]any{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "DELETE fiz FROM foo WHERE bar = ? IF buz = ?",
			wantArgs: []any{3, 2},
		},
	} {
		stc.assert(t)
	}
}
