package gocql

import (
	"context"
	"errors"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/upfluence/cql"
)

type DB struct {
	sess *gocql.Session
}

func NewDB(sess *gocql.Session) *DB {
	return &DB{sess: sess}
}

func trimValues(vs []interface{}) ([]interface{}, []func(*gocql.Query) *gocql.Query) {
	var (
		args []interface{}
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

func (db *DB) query(stmt string, vs []interface{}) *gocql.Query {
	var (
		vvs, fns = trimValues(vs)
		q        = db.sess.Query(stmt, vvs...)
	)

	for _, fn := range fns {
		q = fn(q)
	}

	return q
}

func (db *DB) Exec(ctx context.Context, stmt string, vs ...interface{}) error {
	return db.query(stmt, vs).ExecContext(ctx)
}

func (db *DB) ExecCAS(ctx context.Context, stmt string, vs ...interface{}) cql.CASScanner {
	return &scanner{sc: db.query(stmt, vs), ctx: ctx}
}

func (db *DB) QueryRow(ctx context.Context, stmt string, vs ...interface{}) cql.Scanner {
	return &scanner{sc: db.query(stmt, vs), ctx: ctx}
}

type scanner struct {
	sc *gocql.Query

	ctx context.Context
}

func (s *scanner) ScanCAS(vs ...interface{}) (bool, error) {
	return s.sc.ScanCASContext(s.ctx, vs...)
}

func (s *scanner) Scan(vs ...interface{}) error {
	if err := s.sc.ScanContext(s.ctx, vs...); !errors.Is(err, gocql.ErrNotFound) {
		return err
	}

	return cql.ErrNoRows
}

type cursor struct {
	*gocql.Iter
}

func (db *DB) Query(ctx context.Context, stmt string, vs ...interface{}) cql.Cursor {
	return cursor{db.query(stmt, vs).IterContext(ctx)}
}

type batch struct {
	*gocql.Batch

	ctx context.Context
}

func (b *batch) Query(stmt string, vs ...interface{}) {
	b.Batch = b.Batch.Query(stmt, vs...)
}

func (b batch) Exec() error {
	return b.ExecContext(b.ctx)
}

func (b batch) ExecCAS() (bool, cql.Cursor, error) {
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
