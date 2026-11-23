package stats

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/errors"
	statslib "github.com/upfluence/stats"

	"github.com/upfluence/cql"
)

const (
	failureCase = "failure"
	failed      = "failed"
	queryString = "query"
	statusLabel = "status"
	success     = "success"
	successCase = "success"
)

var (
	errQuery = errors.New("query")

	instrumentOptions = []statslib.InstrumentOption{
		statslib.DisableDurationTracking(),
		statslib.DisableStartedCounter(),
	}
)

type fakeScanner struct {
	err     error
	applied bool
}

func (s *fakeScanner) Scan(...any) error { return s.err }

func (s *fakeScanner) ScanCAS(...any) (bool, error) { return s.applied, s.err }

type fakeCursor struct {
	rows     int
	closeErr error
}

func (c *fakeCursor) Scan(...any) bool {
	if c.rows == 0 {
		return false
	}

	c.rows--

	return true
}

func (c *fakeCursor) Close() error { return c.closeErr }

type fakeBatch struct {
	queries []string
	err     error
	applied bool
}

func (b *fakeBatch) Query(stmt string, _ ...any) { b.queries = append(b.queries, stmt) }

func (b *fakeBatch) Exec() error { return b.err }

func (b *fakeBatch) ExecCAS() (bool, cql.Cursor, error) { return b.applied, &fakeCursor{}, b.err }

type fakeDB struct {
	err     error
	scanner *fakeScanner
	cursor  *fakeCursor
	batch   *fakeBatch

	stmts []string
}

func (db *fakeDB) Exec(_ context.Context, stmt string, _ ...any) error {
	db.stmts = append(db.stmts, stmt)

	return db.err
}

func (db *fakeDB) ExecCAS(_ context.Context, stmt string, _ ...any) cql.CASScanner {
	db.stmts = append(db.stmts, stmt)

	return db.scanner
}

func (db *fakeDB) QueryRow(_ context.Context, stmt string, _ ...any) cql.Scanner {
	db.stmts = append(db.stmts, stmt)

	return db.scanner
}

func (db *fakeDB) Query(_ context.Context, stmt string, _ ...any) cql.Cursor {
	db.stmts = append(db.stmts, stmt)

	return db.cursor
}

func (db *fakeDB) Batch(context.Context, cql.BatchType, ...cql.Option) cql.Batch {
	return db.batch
}

func testConfig() Config {
	return Config{
		ExecOptions:         instrumentOptions,
		ExecCASOptions:      instrumentOptions,
		QueryOptions:        instrumentOptions,
		QueryRowOptions:     instrumentOptions,
		BatchExecOptions:    instrumentOptions,
		BatchExecCASOptions: instrumentOptions,
		TrackRowsScanned:    true,
		TrackBatchQueries:   true,
	}
}

func wrap(rawDB cql.DB) (cql.DB, *statslib.StaticCollector) {
	collector := statslib.NewStaticCollector()
	db := NewFactory(statslib.RootScope(collector), testConfig()).Wrap(rawDB)

	return db, collector
}

func metric(name, status string) statslib.Int64Snapshot {
	return statslib.Int64Snapshot{
		Name:   name,
		Labels: map[string]string{statusLabel: status},
		Value:  1,
	}
}

func counters(rowsScanned, batchQueries int64, metrics ...statslib.Int64Snapshot) []statslib.Int64Snapshot {
	metrics = append(
		metrics,
		statslib.Int64Snapshot{Name: "batch_queries_total", Labels: map[string]string{}, Value: batchQueries},
		statslib.Int64Snapshot{Name: "rows_scanned_total", Labels: map[string]string{}, Value: rowsScanned},
	)
	sort.Sort(statslib.Int64Snapshots(metrics))

	return metrics
}

func TestUnwrap(t *testing.T) {
	rawDB := &fakeDB{}
	db, _ := wrap(rawDB)

	u, ok := db.(interface{ Unwrap() cql.DB })

	require.True(t, ok)
	assert.Same(t, rawDB, u.Unwrap())
}

func TestWithoutTracking(t *testing.T) {
	rawDB := &fakeDB{
		cursor: &fakeCursor{rows: 2},
		batch:  &fakeBatch{},
	}
	cfg := testConfig()
	cfg.TrackRowsScanned = false
	cfg.TrackBatchQueries = false
	collector := statslib.NewStaticCollector()
	db := NewFactory(statslib.RootScope(collector), cfg).Wrap(rawDB)

	cursor := db.Query(context.Background(), queryString)

	for cursor.Scan() {
	}

	require.NoError(t, cursor.Close())

	b := db.Batch(context.Background(), cql.LoggedBatch)
	b.Query(queryString)

	require.NoError(t, b.Exec())
	assert.Equal(
		t,
		[]statslib.Int64Snapshot{
			metric("batch_exec_total", success),
			metric("query_total", success),
		},
		collector.Get().Counters,
	)
}
