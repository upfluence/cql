package scroller

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/x/cqlbuilder"
)

var errClose = errors.New("close failed")

type collector struct {
	mu sync.Mutex

	rows   []scrolledRow
	closes int
}

func (c *collector) scanner(closeErr error) RangeScanner {
	return &rangeCollector{c: c, closeErr: closeErr}
}

type rangeCollector struct {
	c        *collector
	rows     []scrolledRow
	closeErr error
}

func (rc *rangeCollector) Scan(sc cqlbuilder.Scanner) error {
	r, err := scanRow(sc)

	if err != nil {
		return err
	}

	rc.rows = append(rc.rows, r)

	return nil
}

func (rc *rangeCollector) Close() error {
	rc.c.mu.Lock()
	defer rc.c.mu.Unlock()

	rc.c.rows = append(rc.c.rows, rc.rows...)
	rc.c.closes++

	return rc.closeErr
}

func rowStatement(tr *TokenRange) Statement {
	return Statement{
		Table:       table,
		PrimaryKeys: partitionKey,
		Fields:      []cqlbuilder.Marker{cqlbuilder.Column("c")},
		ScrollSize:  NoLimit,
		Range:       tr,
	}
}

func TestConcurrentScroll(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		var halfRange = FullTokenRange.Split(2)[0]

		for _, tt := range []struct {
			name       string
			haveStmt   Statement
			haveOpts   []ConcurrentOption
			wantRows   []scrolledRow
			wantCloses int
		}{
			{
				name:       "default options",
				haveStmt:   rowStatement(nil),
				wantRows:   allRows(),
				wantCloses: 4 * runtime.GOMAXPROCS(0),
			},
			{
				name:       "splits and concurrency",
				haveStmt:   rowStatement(nil),
				haveOpts:   []ConcurrentOption{WithSplits(8), WithConcurrency(3)},
				wantRows:   allRows(),
				wantCloses: 8,
			},
			{
				name:       "within a token range",
				haveStmt:   rowStatement(&halfRange),
				haveOpts:   []ConcurrentOption{WithSplits(4), WithConcurrency(0)},
				wantRows:   scrollRows(t, db, Statement{ScrollSize: NoLimit, Range: &halfRange}),
				wantCloses: 4,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var (
					c collector
					s = Scroller{QueryBuilder: cqlbuilder.QueryBuilder{DB: db}}
				)

				err := ConcurrentScroll(
					context.Background(),
					&s,
					tt.haveStmt,
					func(TokenRange) RangeScanner { return c.scanner(nil) },
					tt.haveOpts...,
				)

				require.NoError(t, err)
				assert.ElementsMatch(t, tt.wantRows, c.rows)
				assert.Equal(t, tt.wantCloses, c.closes)
			})
		}
	})
}

func TestConcurrentScrollRangeError(t *testing.T) {
	integrationTest(t, func(t *testing.T, db cql.DB) {
		var (
			c           collector
			failedRange = SplitTokenRange(8)[2]
			s           = Scroller{QueryBuilder: cqlbuilder.QueryBuilder{DB: db}}
		)

		err := ConcurrentScroll(
			context.Background(),
			&s,
			rowStatement(nil),
			func(tr TokenRange) RangeScanner {
				if tr == failedRange {
					return c.scanner(errClose)
				}

				return c.scanner(nil)
			},
			WithSplits(8),
			WithConcurrency(3),
		)

		var re *RangeError

		require.ErrorAs(t, err, &re)
		assert.Equal(t, failedRange, re.Range)
		require.ErrorIs(t, err, errClose)
		assert.ElementsMatch(t, allRows(), c.rows)
		assert.Equal(t, 8, c.closes)
	})
}
