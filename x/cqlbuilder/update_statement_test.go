package cqlbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqltest"
	"github.com/upfluence/cql/x/migration"
)

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
		{
			name: "SettAdd",
			stmt: UpdateStatement{
				Table: "foo",
				UpdateClauses: []UpdateClause{
					{Field: Column("fiz"), Op: SetAdd},
				},
				WhereClause: In(Column("bar")),
			},
			vs:       map[string]interface{}{"fiz": []int{1}, "bar": []int{3, 4}},
			wantStmt: "UPDATE foo SET fiz = fiz + ? WHERE bar IN (?, ?)",
			wantArgs: []interface{}{[]int{1}, 3, 4},
		},
	} {
		stc.assert(t)
	}
}

func TestIntegrationSet(t *testing.T) {
	cqltest.NewTestCase(
		cqltest.WithMigratorFunc(func(db cql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				cqltest.StaticSource{
					MigrationUp:   "CREATE TABLE IF NOT EXISTS fuz(foo text PRIMARY KEY, bar set<ascii>)",
					MigrationDown: "DROP TABLE fuz",
				},
				migration.MigrationTable("cqlbuilder_set_integration_migrations"),
			)
		}),
	).Run(t, func(t *testing.T, db cql.DB) {
		qb := QueryBuilder{DB: db}

		ue := qb.PrepareUpdate(
			UpdateStatement{
				Table:         "fuz",
				UpdateClauses: []UpdateClause{{Field: Column("bar"), Op: SetAdd}},
				WhereClause:   Eq(Column("foo")),
			},
		)

		err := ue.Exec(
			context.Background(),
			map[string]interface{}{"foo": "foo", "bar": []string{"foo"}},
		)

		assert.NoError(t, err)

		err = ue.Exec(
			context.Background(),
			map[string]interface{}{"foo": "foo", "bar": []string{"foo"}},
		)

		assert.NoError(t, err)
	})
}
