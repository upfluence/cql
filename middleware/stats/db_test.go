package stats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/upfluence/cql"
)

func TestExec(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
	}{
		{name: successCase, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &fakeDB{err: tt.haveErr}
			db, collector := wrap(rawDB)

			err := db.Exec(context.Background(), queryString, 42)

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, []string{queryString}, rawDB.stmts)
			assert.Equal(t, counters(0, 0, metric("exec_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestExecCAS(t *testing.T) {
	for _, tt := range []struct {
		name        string
		haveErr     error
		haveApplied bool
		wantStatus  string
	}{
		{name: successCase, haveApplied: true, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &fakeDB{scanner: &fakeScanner{err: tt.haveErr, applied: tt.haveApplied}}
			db, collector := wrap(rawDB)

			scanner := db.ExecCAS(context.Background(), queryString, 42)

			assert.Equal(t, counters(0, 0), collector.Get().Counters)

			applied, err := scanner.ScanCAS()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, tt.haveApplied, applied)
			assert.Equal(t, []string{queryString}, rawDB.stmts)
			assert.Equal(t, counters(0, 0, metric("exec_cas_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestQueryRow(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
	}{
		{name: successCase, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &fakeDB{scanner: &fakeScanner{err: tt.haveErr}}
			db, collector := wrap(rawDB)

			scanner := db.QueryRow(context.Background(), queryString, 42)

			assert.Equal(t, counters(0, 0), collector.Get().Counters)

			err := scanner.Scan()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, []string{queryString}, rawDB.stmts)
			assert.Equal(t, counters(0, 0, metric("query_row_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestQueryRowFinishesOnce(t *testing.T) {
	db, collector := wrap(&fakeDB{scanner: &fakeScanner{}})

	scanner := db.QueryRow(context.Background(), queryString)

	require.NoError(t, scanner.Scan())
	require.NoError(t, scanner.Scan())
	assert.Equal(t, counters(0, 0, metric("query_row_total", success)), collector.Get().Counters)
}

func TestQuery(t *testing.T) {
	for _, tt := range []struct {
		name        string
		haveRows    int
		haveErr     error
		wantStatus  string
		wantScanned int64
	}{
		{name: successCase, haveRows: 2, wantStatus: success, wantScanned: 2},
		{name: "empty", wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawDB := &fakeDB{cursor: &fakeCursor{rows: tt.haveRows, closeErr: tt.haveErr}}
			db, collector := wrap(rawDB)

			cursor := db.Query(context.Background(), queryString, 42)

			for cursor.Scan() {
			}

			assert.Equal(t, counters(tt.wantScanned, 0), collector.Get().Counters)

			err := cursor.Close()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, []string{queryString}, rawDB.stmts)
			assert.Equal(
				t,
				counters(tt.wantScanned, 0, metric("query_total", tt.wantStatus)),
				collector.Get().Counters,
			)
		})
	}
}

func TestQueryFinishesOnce(t *testing.T) {
	db, collector := wrap(&fakeDB{cursor: &fakeCursor{}})

	cursor := db.Query(context.Background(), queryString)

	require.NoError(t, cursor.Close())
	require.NoError(t, cursor.Close())
	assert.Equal(t, counters(0, 0, metric("query_total", success)), collector.Get().Counters)
}

func TestBatchExec(t *testing.T) {
	for _, tt := range []struct {
		name       string
		haveErr    error
		wantStatus string
	}{
		{name: successCase, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawBatch := &fakeBatch{err: tt.haveErr}
			db, collector := wrap(&fakeDB{batch: rawBatch})

			b := db.Batch(context.Background(), cql.LoggedBatch)
			b.Query("q1", 1)
			b.Query("q2", 2)

			assert.Equal(t, counters(0, 0), collector.Get().Counters)

			err := b.Exec()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, []string{"q1", "q2"}, rawBatch.queries)
			assert.Equal(t, counters(0, 2, metric("batch_exec_total", tt.wantStatus)), collector.Get().Counters)
		})
	}
}

func TestBatchExecCAS(t *testing.T) {
	for _, tt := range []struct {
		name        string
		haveErr     error
		haveApplied bool
		wantStatus  string
	}{
		{name: successCase, haveApplied: true, wantStatus: success},
		{name: failureCase, haveErr: errQuery, wantStatus: failed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rawBatch := &fakeBatch{err: tt.haveErr, applied: tt.haveApplied}
			db, collector := wrap(&fakeDB{batch: rawBatch})

			b := db.Batch(context.Background(), cql.LoggedBatch)
			b.Query("q1", 1)

			applied, cursor, err := b.ExecCAS()

			assert.Equal(t, tt.haveErr, err)
			assert.Equal(t, tt.haveApplied, applied)
			assert.Equal(t, &fakeCursor{}, cursor)
			assert.Equal(t, []string{"q1"}, rawBatch.queries)
			assert.Equal(
				t,
				counters(0, 1, metric("batch_exec_cas_total", tt.wantStatus)),
				collector.Get().Counters,
			)
		})
	}
}
