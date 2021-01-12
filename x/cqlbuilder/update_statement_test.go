package cqlbuilder

import "testing"

func TestUpdateStatement(t *testing.T) {
	for _, stc := range []statementTestCase{
		{
			name: "basic",
			stmt: UpdateStatement{
				Table: "foo",
				UpdateClauses: []UpdateClause{
					{Field: Column("fiz"), Op: Set},
					{Field: Column("buz"), Op: Set},
				},
				WhereClause: Eq(Column("bar")),
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "UPDATE foo SET fiz = ?, buz = ? WHERE bar = ?",
			wantArgs: []interface{}{1, 2, 3},
		},
		{
			name: "complex lwt",
			stmt: UpdateStatement{
				Table: "foo",
				UpdateClauses: []UpdateClause{
					{Field: Column("fiz"), Op: Set},
				},
				WhereClause: Eq(Column("bar")),
				LWTClause:   PredicateLWTClause{Predicate: Eq(Column("buz"))},
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "UPDATE foo SET fiz = ? WHERE bar = ? IF buz = ?",
			wantArgs: []interface{}{1, 3, 2},
		},
	} {
		stc.assert(t)
	}
}
