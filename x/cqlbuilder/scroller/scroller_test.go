package scroller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqltest"
	"github.com/upfluence/cql/x/cqlbuilder"
	"github.com/upfluence/cql/x/migration"
)

const (
	partitions       = 20
	rowsPerPartition = 3

	table       = "scroll"
	pkColumn    = "pk"
	otherColumn = "other"
)

var partitionKey = []string{pkColumn, otherColumn}

// row is a scrolled row, the columns which are not selected are left empty.
type row struct {
	PK     int
	Other  int
	C      int
	Static string
}

func integrationTest(t *testing.T, fn func(*testing.T, cql.DB)) {
	cqltest.NewTestCase(
		cqltest.WithMigratorFunc(func(db cql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				cqltest.StaticSource{
					MigrationUp: `CREATE TABLE IF NOT EXISTS scroll (
						pk int,
						other int,
						c int,
						st text static,
						PRIMARY KEY ((pk, other), c)
					)`,
					MigrationDown: "DROP TABLE scroll",
				},
				migration.MigrationTable("scroller_integration_migrations"),
			)
		}),
	).Run(t, func(t *testing.T, db cql.DB) {
		for i := range partitions {
			for c := range rowsPerPartition {
				require.NoError(
					t,
					db.Exec(
						context.Background(),
						"INSERT INTO scroll (pk, other, c, st) VALUES (?, ?, ?, ?)",
						i,
						i%2,
						c,
						fmt.Sprintf("p%d", i),
					),
				)
			}
		}

		fn(t, db)
	})
}

// partitionRows returns one row per partition, as scrolled with Distinct.
func partitionRows(filter func(row) bool) []row {
	var rs []row

	for i := range partitions {
		if r := (row{PK: i, Other: i % 2}); filter(r) {
			rs = append(rs, r)
		}
	}

	return rs
}

// clusteringRows returns every row of the table, as scrolled with the c column.
func clusteringRows(filter func(row) bool) []row {
	var rs []row

	for i := range partitions {
		for c := range rowsPerPartition {
			if r := (row{PK: i, Other: i % 2, C: c}); filter(r) {
				rs = append(rs, r)
			}
		}
	}

	return rs
}

func withStatic(rs []row) []row {
	for i := range rs {
		rs[i].Static = fmt.Sprintf("p%d", rs[i].PK)
	}

	return rs
}

func all(row) bool { return true }

func scanRow(sc cqlbuilder.Scanner) (row, error) {
	var r row

	err := sc.Scan(
		map[string]any{
			pkColumn:    &r.PK,
			otherColumn: &r.Other,
			"c":         &r.C,
			"st":        &r.Static,
		},
	)

	return r, err
}

// scrollRows scrolls stmt on the test table once per range, or once on its own
// range when there is none.
func scrollRows(t *testing.T, db cql.DB, stmt Statement, ranges []TokenRange) []row {
	t.Helper()

	stmt.Table = table
	stmt.PrimaryKeys = partitionKey

	var (
		rs []row
		s  = Scroller{QueryBuilder: cqlbuilder.QueryBuilder{DB: db}}

		stmts = []Statement{stmt}
	)

	if len(ranges) > 0 {
		stmts = make([]Statement, len(ranges))

		for i := range ranges {
			stmts[i] = stmt
			stmts[i].Range = &ranges[i]
		}
	}

	for _, stmt := range stmts {
		err := s.Scroll(
			context.Background(),
			stmt,
			func(sc cqlbuilder.Scanner) error {
				r, err := scanRow(sc)

				if err != nil {
					return err
				}

				rs = append(rs, r)

				return nil
			},
		)

		require.NoError(t, err)
	}

	return rs
}

func wantRows(want []row) func(*testing.T, []row) {
	return func(t *testing.T, got []row) {
		t.Helper()

		assert.ElementsMatch(t, want, got)
	}
}

func TestScroll(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		for _, tt := range []struct {
			name       string
			haveStmt   Statement
			haveRanges []TokenRange
			assertRows func(*testing.T, []row)
		}{
			{
				name: "distinct partitions over many pages",
				haveStmt: Statement{
					Distinct:   true,
					ScrollSize: 3,
				},
				assertRows: wantRows(partitionRows(all)),
			},
			{
				name: "distinct partitions with a static column",
				haveStmt: Statement{
					Fields:     []cqlbuilder.Marker{cqlbuilder.Column("st")},
					Distinct:   true,
					ScrollSize: 7,
				},
				assertRows: wantRows(withStatic(partitionRows(all))),
			},
			{
				name: "distinct partitions filtered on a partition key column",
				haveStmt: Statement{
					Distinct:       true,
					ScrollSize:     3,
					WhereClause:    cqlbuilder.Eq(cqlbuilder.Column(otherColumn)),
					Values:         map[string]any{otherColumn: 1},
					AllowFiltering: true,
				},
				assertRows: wantRows(partitionRows(func(r row) bool { return r.Other == 1 })),
			},
			{
				name: "distinct partitions within token ranges",
				haveStmt: Statement{
					Distinct:   true,
					ScrollSize: 2,
				},
				haveRanges: SplitTokenRange(4),
				assertRows: wantRows(partitionRows(all)),
			},
			{
				name: "rows filtered on a clustering column",
				haveStmt: Statement{
					Fields:         []cqlbuilder.Marker{cqlbuilder.Column("c")},
					WhereClause:    cqlbuilder.StaticEq(cqlbuilder.Column("c"), 1),
					AllowFiltering: true,
				},
				assertRows: wantRows(clusteringRows(func(r row) bool { return r.C == 1 })),
			},
			{
				name: "rows with a page limit skip the end of partitions",
				haveStmt: Statement{
					Fields:     []cqlbuilder.Marker{cqlbuilder.Column("c")},
					ScrollSize: 4,
				},
				assertRows: func(t *testing.T, got []row) {
					assert.Less(t, len(got), len(clusteringRows(all)))
				},
			},
			{
				name: "rows without limit",
				haveStmt: Statement{
					Fields:     []cqlbuilder.Marker{cqlbuilder.Column("c")},
					ScrollSize: NoLimit,
				},
				assertRows: wantRows(clusteringRows(all)),
			},
			{
				name: "rows without limit within token ranges",
				haveStmt: Statement{
					Fields:     []cqlbuilder.Marker{cqlbuilder.Column("c")},
					ScrollSize: NoLimit,
				},
				haveRanges: SplitTokenRange(4),
				assertRows: wantRows(clusteringRows(all)),
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				tt.assertRows(t, scrollRows(t, db, tt.haveStmt, tt.haveRanges))
			})
		}
	})
}
