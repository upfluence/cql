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
	partitions        = 20
	rowsPerPartitions = 3
)

const (
	table       = "scroll"
	pkColumn    = "pk"
	otherColumn = "other"
)

var partitionKey = []string{pkColumn, otherColumn}

type partition struct {
	PK     int
	Other  int
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
			for c := range rowsPerPartitions {
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

func allPartitions(filter func(partition) bool) []partition {
	var ps []partition

	for i := range partitions {
		if p := (partition{PK: i, Other: i % 2}); filter(p) {
			ps = append(ps, p)
		}
	}

	return ps
}

func withStatic(ps []partition) []partition {
	for i := range ps {
		ps[i].Static = fmt.Sprintf("p%d", ps[i].PK)
	}

	return ps
}

func scrollPartitions(t *testing.T, db cql.DB, stmt Statement) []partition {
	t.Helper()

	var (
		ps []partition
		s  = Scroller{QueryBuilder: cqlbuilder.QueryBuilder{DB: db}}
	)

	err := s.Scroll(
		context.Background(),
		stmt,
		func(sc cqlbuilder.Scanner) error {
			var p partition

			err := sc.Scan(
				map[string]any{pkColumn: &p.PK, otherColumn: &p.Other, "st": &p.Static},
			)

			if err != nil {
				return err
			}

			ps = append(ps, p)

			return nil
		},
	)

	require.NoError(t, err)

	return ps
}

func TestScroll(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		for _, tt := range []struct {
			name string
			stmt Statement
			want []partition
		}{
			{
				name: "distinct partitions over many pages",
				stmt: Statement{
					Table:       table,
					PrimaryKeys: partitionKey,
					Distinct:    true,
					ScrollSize:  3,
				},
				want: allPartitions(func(partition) bool { return true }),
			},
			{
				name: "distinct partitions with a static column",
				stmt: Statement{
					Table:       table,
					PrimaryKeys: partitionKey,
					Fields:      []cqlbuilder.Marker{cqlbuilder.Column("st")},
					Distinct:    true,
					ScrollSize:  7,
				},
				want: withStatic(allPartitions(func(partition) bool { return true })),
			},
			{
				name: "distinct partitions filtered on a partition key column",
				stmt: Statement{
					Table:          table,
					PrimaryKeys:    partitionKey,
					Distinct:       true,
					ScrollSize:     3,
					WhereClause:    cqlbuilder.Eq(cqlbuilder.Column(otherColumn)),
					Values:         map[string]any{otherColumn: 1},
					AllowFiltering: true,
				},
				want: allPartitions(func(p partition) bool { return p.Other == 1 }),
			},
			{
				name: "rows filtered on a clustering column",
				stmt: Statement{
					Table:          table,
					PrimaryKeys:    partitionKey,
					ScrollSize:     3,
					WhereClause:    cqlbuilder.StaticEq(cqlbuilder.Column("c"), 1),
					AllowFiltering: true,
				},
				want: allPartitions(func(partition) bool { return true }),
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				assert.ElementsMatch(t, tt.want, scrollPartitions(t, db, tt.stmt))
			})
		}
	})
}

func TestScrollTokenRanges(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		var ps []partition

		for _, tr := range SplitTokenRange(4) {
			ps = append(
				ps,
				scrollPartitions(
					t,
					db,
					Statement{
						Table:       table,
						PrimaryKeys: partitionKey,
						Distinct:    true,
						ScrollSize:  2,
						Range:       &tr,
					},
				)...,
			)
		}

		assert.ElementsMatch(t, allPartitions(func(partition) bool { return true }), ps)
	})
}

type scrolledRow struct {
	PK    int
	Other int
	C     int
}

func allRows() []scrolledRow {
	var rs []scrolledRow

	for i := range partitions {
		for c := range rowsPerPartitions {
			rs = append(rs, scrolledRow{PK: i, Other: i % 2, C: c})
		}
	}

	return rs
}

func scrollRows(t *testing.T, db cql.DB, stmt Statement) []scrolledRow {
	t.Helper()

	var (
		rs []scrolledRow
		s  = Scroller{QueryBuilder: cqlbuilder.QueryBuilder{DB: db}}
	)

	stmt.Table = table
	stmt.PrimaryKeys = partitionKey
	stmt.Fields = []cqlbuilder.Marker{cqlbuilder.Column("c")}

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

	return rs
}

func TestScrollRows(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		t.Run("page limit skips the end of partitions", func(t *testing.T) {
			assert.Less(t, len(scrollRows(t, db, Statement{ScrollSize: 4})), len(allRows()))
		})

		t.Run("no limit", func(t *testing.T) {
			assert.ElementsMatch(t, allRows(), scrollRows(t, db, Statement{ScrollSize: NoLimit}))
		})

		t.Run("no limit within token ranges", func(t *testing.T) {
			var rs []scrolledRow

			for _, tr := range SplitTokenRange(4) {
				rs = append(rs, scrollRows(t, db, Statement{ScrollSize: NoLimit, Range: &tr})...)
			}

			assert.ElementsMatch(t, allRows(), rs)
		})
	})
}

func scanRow(sc cqlbuilder.Scanner) (scrolledRow, error) {
	var r scrolledRow

	err := sc.Scan(map[string]any{pkColumn: &r.PK, otherColumn: &r.Other, "c": &r.C})

	return r, err
}
