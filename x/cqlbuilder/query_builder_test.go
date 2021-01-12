package cqlbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqltest"
	"github.com/upfluence/cql/x/migration"
)

type statementTestCase struct {
	name string

	stmt statement
	vs   map[string]interface{}

	wantStmt string
	wantArgs []interface{}
	wantErr  error
}

func (stc statementTestCase) assert(t *testing.T) {
	t.Helper()

	t.Run(stc.name, func(t *testing.T) {
		t.Helper()

		stmt, args, err := stc.stmt.buildQuery(stc.vs)

		assert.Equal(t, stc.wantStmt, stmt)
		assert.Equal(t, stc.wantArgs, args)
		assert.Equal(t, stc.wantErr, err)
	})
}

func integrationTest(t *testing.T, fn func(*testing.T, cql.DB)) {
	cqltest.NewTestCase(
		cqltest.WithMigratorFunc(func(db cql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				cqltest.StaticSource{
					MigrationUp:   "CREATE TABLE IF NOT EXISTS fuz(foo text PRIMARY KEY, bar blob)",
					MigrationDown: "DROP TABLE fuz",
				},
				migration.MigrationTable("cqlbuilder_integration_migrations"),
			)
		}),
	).Run(t, fn)
}

func TestCAS(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		qb := QueryBuilder{DB: db}

		ie := qb.PrepareInsert(
			InsertStatement{
				Table:     "fuz",
				Fields:    []Marker{Column("foo"), Column("bar")},
				LWTClause: NotExistsClause,
			},
		)

		ok, _, _, err := execCAS(ie, "foo", "bar")
		assert.True(t, ok)
		assert.NoError(t, err)

		ok, _, bar, err := execCAS(ie, "foo", "foo")
		assert.False(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "bar", bar)

		ue := qb.PrepareUpdate(
			UpdateStatement{
				Table:         "fuz",
				UpdateClauses: []UpdateClause{{Field: Column("bar"), Op: Set}},
				WhereClause:   Eq(Column("foo")),
				LWTClause: PredicateLWTClause{
					Predicate: StaticEq(Column("bar"), "bar"),
				},
			},
		)

		ok, _, _, err = execCAS(ue, "foo", "foo")
		assert.True(t, ok)
		assert.NoError(t, err)

		ok, _, bar, err = execCAS(ue, "foo", "foo")
		assert.False(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "foo", bar)

		de := qb.PrepareDelete(
			DeleteStatement{
				Table:       "fuz",
				WhereClause: Eq(Column("foo")),
				LWTClause: PredicateLWTClause{
					Predicate: StaticEq(Column("bar"), "foo"),
				},
			},
		)

		ok, _, _, err = execCAS(de, "foo", "")
		assert.True(t, ok)
		assert.NoError(t, err)
	})
}

func TestEC(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		qb := QueryBuilder{DB: db}

		se := qb.PrepareSelect(
			SelectStatement{
				Table:         "fuz",
				SelectClauses: []Marker{Column("bar")},
				WhereClause:   Eq(Column("foo")),
			},
		)

		bar, err := queryRow(se, "foo")
		assert.Equal(t, cql.ErrNoRows, err)
		assert.Equal(t, "", bar)

		ie := qb.PrepareInsert(
			InsertStatement{
				Table:  "fuz",
				Fields: []Marker{Column("foo"), Column("bar")},
			},
		)

		err = exec(ie, "foo", "bar")
		assert.NoError(t, err)

		bar, err = queryRow(se, "foo")
		assert.NoError(t, err)
		assert.Equal(t, "bar", bar)

		ue := qb.PrepareUpdate(
			UpdateStatement{
				Table:         "fuz",
				UpdateClauses: []UpdateClause{{Field: Column("bar"), Op: Set}},
				WhereClause:   Eq(Column("foo")),
			},
		)

		err = exec(ue, "foo", "foo")
		assert.NoError(t, err)

		bar, err = queryRow(se, "foo")
		assert.NoError(t, err)
		assert.Equal(t, "foo", bar)

		de := qb.PrepareDelete(
			DeleteStatement{Table: "fuz", WhereClause: Eq(Column("foo"))},
		)

		err = exec(de, "foo", "")
		assert.NoError(t, err)

		bar, err = queryRow(se, "foo")
		assert.Equal(t, cql.ErrNoRows, err)
		assert.Equal(t, "", bar)
	})
}

func TestBatch(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		qb := QueryBuilder{DB: db}

		be := qb.PrepareBatch(
			BatchStatement{
				Type: cql.LoggedBatch,
				Statements: []CASStatement{
					InsertStatement{
						Table:  "fuz",
						Fields: []Marker{CQLExpression("foo1", "foo"), Column("bar")},
					},
					InsertStatement{
						Table:  "fuz",
						Fields: []Marker{CQLExpression("foo2", "foo"), Column("bar")},
					},
				},
			},
		)

		err := be.Exec(
			context.Background(),
			map[string]interface{}{"foo1": "foo", "foo2": "bar", "bar": "buz"},
		)

		assert.NoError(t, err)

		cur := qb.PrepareSelect(
			SelectStatement{
				Table:         "fuz",
				SelectClauses: []Marker{Column("foo"), Column("bar")},
			},
		).Query(context.Background(), nil)

		var foo, bar string

		vs := make(map[string]string)

		for cur.Scan(map[string]interface{}{"foo": &foo, "bar": &bar}) {
			vs[foo] = bar
		}

		assert.NoError(t, cur.Close())
		assert.Equal(t, map[string]string{"foo": "buz", "bar": "buz"}, vs)
	})
}

func queryRow(sq *SelectQueryer, foo string) (string, error) {
	var bar string

	return bar, sq.QueryRow(
		context.Background(),
		map[string]interface{}{"foo": foo},
	).Scan(map[string]interface{}{"bar": &bar})
}

func exec(e Execer, foo, bar string) error {
	return e.Exec(
		context.Background(),
		map[string]interface{}{"foo": foo, "bar": bar},
	)
}

func execCAS(e Execer, foo, bar string) (bool, string, string, error) {
	var outFoo, outBar string

	ok, err := e.ExecCAS(
		context.Background(),
		map[string]interface{}{"foo": foo, "bar": bar},
	).ScanCAS(
		map[string]interface{}{"foo": &outFoo, "bar": &outBar},
	)

	return ok, outFoo, outBar, err
}
