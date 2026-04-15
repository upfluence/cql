// Package gocql provides an implementation of the cql.DB interface using the Apache Cassandra gocql driver.
package gocql

import (
	"context"
	"errors"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/upfluence/cql"
)

// DB is a Cassandra database implementation that wraps a gocql.Session.
// It implements the cql.DB interface and translates operations to the underlying gocql driver.
type DB struct {
	sess *gocql.Session
}

func NewDB(sess *gocql.Session) *DB {
	return &DB{sess: sess}
}

func trimValues(vs []any) ([]any, []func(*gocql.Query) *gocql.Query) {
	// Fast path: no options present, return the original slice as-is.
	hasOption := false

	for _, v := range vs {
		if _, ok := v.(cql.Option); ok {
			hasOption = true
			break
		}
	}

	if !hasOption {
		return vs, nil
	}

	var (
		args []any
		fns  []func(*gocql.Query) *gocql.Query
	)

	for _, v := range vs {
		switch vv := v.(type) {
		case cql.WithConsistency:
			fns = append(
				fns,
				func(q *gocql.Query) *gocql.Query { return q.Consistency(gocql.Consistency(vv)) },
			)
		case cql.Option:
		default:
			args = append(args, vv)
		}
	}

	return args, fns
}

func (db *DB) Session() *gocql.Session { return db.sess }

func (db *DB) query(stmt string, vs []any) *gocql.Query {
	var (
		vvs, fns = trimValues(vs)
		q        = db.sess.Query(stmt, vvs...)
	)

	for _, fn := range fns {
		q = fn(q)
	}

	return q
}

func (db *DB) Exec(ctx context.Context, stmt string, vs ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return db.query(stmt, vs).ExecContext(ctx)
}

func (db *DB) ExecCAS(ctx context.Context, stmt string, vs ...any) cql.CASScanner {
	return &scanner{sc: db.query(stmt, vs), ctx: ctx}
}

func (db *DB) QueryRow(ctx context.Context, stmt string, vs ...any) cql.Scanner {
	return &scanner{sc: db.query(stmt, vs), ctx: ctx}
}

type scanner struct {
	sc *gocql.Query

	ctx context.Context
}

func (s *scanner) ScanCAS(vs ...any) (bool, error) {
	if err := s.ctx.Err(); err != nil {
		return false, err
	}

	return s.sc.ScanCASContext(s.ctx, vs...)
}

func (s *scanner) Scan(vs ...any) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}

	if err := s.sc.ScanContext(s.ctx, vs...); !errors.Is(err, gocql.ErrNotFound) {
		return err
	}

	return cql.ErrNoRows
}

type cursor struct {
	*gocql.Iter
}

type errCursor struct{ err error }

func (ec errCursor) Scan(...any) bool { return false }
func (ec errCursor) Close() error     { return ec.err }

func (db *DB) Query(ctx context.Context, stmt string, vs ...any) cql.Cursor {
	if err := ctx.Err(); err != nil {
		return errCursor{err}
	}

	return cursor{db.query(stmt, vs).IterContext(ctx)}
}

type batch struct {
	*gocql.Batch

	ctx context.Context
}

func (b *batch) Query(stmt string, vs ...any) {
	b.Batch = b.Batch.Query(stmt, vs...)
}

func (b batch) Exec() error {
	if err := b.ctx.Err(); err != nil {
		return err
	}

	return b.ExecContext(b.ctx)
}

func (b batch) ExecCAS() (bool, cql.Cursor, error) {
	if err := b.ctx.Err(); err != nil {
		return false, nil, err
	}

	ok, iter, err := b.ExecCASContext(b.ctx)

	if err != nil {
		return ok, nil, err
	}

	return ok, cursor{iter}, nil
}

var gocqlBatchTypes = map[cql.BatchType]gocql.BatchType{
	cql.LoggedBatch:   gocql.LoggedBatch,
	cql.UnloggedBatch: gocql.UnloggedBatch,
	cql.CounterBatch:  gocql.CounterBatch,
}

func (db *DB) Batch(ctx context.Context, bt cql.BatchType, opts ...cql.Option) cql.Batch {
	b := db.sess.Batch(gocqlBatchTypes[bt])

	for _, o := range opts {
		if c, ok := o.(cql.WithConsistency); ok {
			b = b.Consistency(gocql.Consistency(c))
		}
	}

	return &batch{Batch: b, ctx: ctx}
}

func GetSession(db cql.DB) *gocql.Session {
	if u, ok := db.(interface{ Unwrap() cql.DB }); ok {
		return GetSession(u.Unwrap())
	}

	if gdb, ok := db.(*DB); ok {
		return gdb.sess
	}

	return nil
}
