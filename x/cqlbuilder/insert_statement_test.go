package cqlbuilder

import "testing"

func TestInsertStatement(t *testing.T) {
	for _, stc := range []statementTestCase{
		{
			name: "basic",
			stmt: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("fiz"), Column("buz")},
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2},
			wantStmt: "INSERT INTO foo(fiz, buz) VALUES (?, ?)",
			wantArgs: []interface{}{1, 2},
		},
		{
			name: "basic",
			stmt: InsertStatement{
				Table:     "foo",
				Fields:    []Marker{Column("fiz"), Column("buz")},
				LWTClause: NotExistsClause,
			},
			vs:       map[string]interface{}{"fiz": 1, "buz": 2},
			wantStmt: "INSERT INTO foo(fiz, buz) VALUES (?, ?) IF NOT EXISTS",
			wantArgs: []interface{}{1, 2},
		},
		{
			name: "missing key",
			stmt: InsertStatement{
				Table:  "foo",
				Fields: []Marker{Column("fiz"), Column("buz")},
			},
			vs:      map[string]interface{}{"fiz": 1},
			wantErr: ErrMissingKey{Key: "buz"},
		},
	} {
		stc.assert(t)
	}
}
