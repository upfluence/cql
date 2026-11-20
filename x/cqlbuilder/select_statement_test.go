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
			vs:       map[string]any{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "SELECT fiz, buz FROM foo WHERE bar = ? ALLOW FILTERING",
			wantArgs: []any{3},
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
			name: "group by",
			stmt: SelectStatement{
				Table: "foo",
				SelectClauses: []Marker{
					Column("fiz"),
					CQLExpression("count", "count(*)"),
				},
				WhereClause:   Eq(Column("bar")),
				GroupByClause: []Marker{Column("fiz"), Column("buz")},
				OrderByClause: OrderByClause{Field: Column("fiz"), Direction: Desc},
				Limit:         NullableInt{Valid: true, Int: 10},
			},
			vs:       map[string]any{"bar": 3},
			wantStmt: "SELECT fiz, count(*) FROM foo WHERE bar = ? GROUP BY fiz, buz ORDER BY fiz DESC LIMIT 10",
			wantArgs: []any{3},
		},
		{
			name: "per partition limit",
			stmt: SelectStatement{
				Table:             "foo",
				SelectClauses:     []Marker{Column("fiz"), Column("buz")},
				WhereClause:       In(Column("bar")),
				PerPartitionLimit: NullableInt{Valid: true, Int: 1},
				Limit:             NullableInt{Valid: true, Int: 10},
			},
			vs:       map[string]any{"bar": []int{1, 2}},
			wantStmt: "SELECT fiz, buz FROM foo WHERE bar IN (?, ?) PER PARTITION LIMIT 1 LIMIT 10",
			wantArgs: []any{1, 2},
		},
		{
			name: "basic and",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause:   And(Eq(Column("bar")), Eq(Column("fiz"))),
			},
			vs:       map[string]any{"fiz": 1, "buz": 2, "bar": 3},
			wantStmt: "SELECT fiz, buz FROM foo WHERE (bar = ?) AND (fiz = ?)",
			wantArgs: []any{3, 1},
		},
		{
			name: "basic and with nested in empty",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause:   And(In(Column("bar")), In(Column("fiz"))),
			},
			vs:      map[string]any{"fiz": []string{}, "bar": []string{"foo"}},
			wantErr: skipClause,
		},
		{
			name: "valued compounded",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause: CompoundedIn(
					"compound_values",
					[]Marker{Column("bar"), Column("fiz")},
				),
			},
			vs: map[string]any{
				"compound_values": []map[string]any{
					{"bar": 1, "fiz": 2},
					{"bar": 3, "fiz": 4},
				},
			},
			wantStmt: "SELECT fiz, buz FROM foo WHERE (bar, fiz) IN ((?, ?), (?, ?))",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "static compounded",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause: StaticCompoundedIn(
					[]Marker{Column("bar"), Column("fiz")},
					[]map[string]any{
						{"bar": 1, "fiz": 2},
						{"bar": 3, "fiz": 4},
					},
				),
			},
			wantStmt: "SELECT fiz, buz FROM foo WHERE (bar, fiz) IN ((?, ?), (?, ?))",
			wantArgs: []any{1, 2, 3, 4},
		},
		{
			name: "static compounded empty",
			stmt: SelectStatement{
				Table:         "foo",
				SelectClauses: []Marker{Column("fiz"), Column("buz")},
				WhereClause: StaticCompoundedIn(
					[]Marker{Column("bar"), Column("fiz")},
					nil,
				),
			},
			wantErr: skipClause,
		},
	} {
		stc.assert(t)
	}
}
