// Package stats instruments CQL database operations.
package stats

import (
	"context"

	"github.com/upfluence/stats"

	"github.com/upfluence/cql"
)

// Config configures each CQL operation instrument.
type Config struct {
	ExecOptions         []stats.InstrumentOption
	ExecCASOptions      []stats.InstrumentOption
	QueryOptions        []stats.InstrumentOption
	QueryRowOptions     []stats.InstrumentOption
	BatchExecOptions    []stats.InstrumentOption
	BatchExecCASOptions []stats.InstrumentOption

	TrackRowsScanned  bool
	TrackBatchQueries bool
}

type instruments struct {
	exec         stats.Instrument
	execCAS      stats.Instrument
	query        stats.Instrument
	queryRow     stats.Instrument
	batchExec    stats.Instrument
	batchExecCAS stats.Instrument

	rowsScanned  stats.Counter
	batchQueries stats.Counter
}

type factory struct {
	instruments instruments
}

// NewFactory creates a CQL middleware factory using scope.
func NewFactory(scope stats.Scope, cfg Config) cql.MiddlewareFactory {
	rowsScanned := stats.NoopCounter

	if cfg.TrackRowsScanned {
		rowsScanned = scope.Counter("rows_scanned_total")
	}

	batchQueries := stats.NoopCounter

	if cfg.TrackBatchQueries {
		batchQueries = scope.Counter("batch_queries_total")
	}

	return &factory{
		instruments: instruments{
			exec:         stats.NewInstrument(scope, "exec", cfg.ExecOptions...),
			execCAS:      stats.NewInstrument(scope, "exec_cas", cfg.ExecCASOptions...),
			query:        stats.NewInstrument(scope, "query", cfg.QueryOptions...),
			queryRow:     stats.NewInstrument(scope, "query_row", cfg.QueryRowOptions...),
			batchExec:    stats.NewInstrument(scope, "batch_exec", cfg.BatchExecOptions...),
			batchExecCAS: stats.NewInstrument(scope, "batch_exec_cas", cfg.BatchExecCASOptions...),

			rowsScanned:  rowsScanned,
			batchQueries: batchQueries,
		},
	}
}

func (f *factory) Wrap(rawDB cql.DB) cql.DB {
	return &db{db: rawDB, instruments: f.instruments}
}

type db struct {
	db cql.DB

	instruments instruments
}

func (d *db) Unwrap() cql.DB {
	return d.db
}

func (d *db) Exec(ctx context.Context, stmt string, vs ...any) error {
	operation := d.instruments.exec.Begin()
	err := d.db.Exec(ctx, stmt, vs...)
	operation.Finish(err)

	return err //nolint:wrapcheck
}

func (d *db) ExecCAS(ctx context.Context, stmt string, vs ...any) cql.CASScanner {
	operation := d.instruments.execCAS.Begin()

	return &casScanner{
		CASScanner: d.db.ExecCAS(ctx, stmt, vs...),
		operation:  operation,
	}
}

func (d *db) QueryRow(ctx context.Context, stmt string, vs ...any) cql.Scanner {
	operation := d.instruments.queryRow.Begin()

	return &scanner{
		Scanner:   d.db.QueryRow(ctx, stmt, vs...),
		operation: operation,
	}
}

func (d *db) Query(ctx context.Context, stmt string, vs ...any) cql.Cursor {
	operation := d.instruments.query.Begin()

	return &cursor{
		Cursor:      d.db.Query(ctx, stmt, vs...),
		operation:   operation,
		rowsScanned: d.instruments.rowsScanned,
	}
}

func (d *db) Batch(ctx context.Context, bt cql.BatchType, opts ...cql.Option) cql.Batch {
	return &batch{
		Batch:       d.db.Batch(ctx, bt, opts...),
		instruments: d.instruments,
	}
}
