package gocql

import (
	"context"

	"github.com/gocql/gocql"

	"github.com/upfluence/cql"
)

type DB struct {
	sess *gocql.Session
}

func NewDB(sess *gocql.Session) *DB {
	return &DB{sess: sess}
}

func trimValues(vs []interface{}) ([]interface{}, []func(*gocql.Query)) {
	var (
		args []interface{}
		fns  []func(*gocql.Query)
	)

	for _, v := range vs {
		switch vv := v.(type) {
		case cql.WithConsistency:
			fns = append(
				fns,
				func(q *gocql.Query) { q.SetConsistency(gocql.Consistency(vv)) },
			)
		case cql.Option:
		default:
			args = append(args, vv)
		}
	}

	return args, fns
}

func (db *DB) Session() *gocql.Session { return db.sess }

func (db *DB) query(ctx context.Context, stmt string, vs []interface{}) *gocql.Query {
	var (
		vvs, fns = trimValues(vs)
		q        = db.sess.Query(stmt, vvs...).WithContext(ctx)
	)

	for _, fn := range fns {
		fn(q)
	}

	return q
}

func (db *DB) Exec(ctx context.Context, stmt string, vs ...interface{}) error {
	return db.query(ctx, stmt, vs).Exec()
}

func (db *DB) ExecCAS(ctx context.Context, stmt string, vs ...interface{}) cql.CASScanner {
	return db.query(ctx, stmt, vs)
}

func (db *DB) QueryRow(ctx context.Context, stmt string, vs ...interface{}) cql.Scanner {
	return db.query(ctx, stmt, vs)
}

type cursor struct {
	*gocql.Iter
}

func (db *DB) Query(ctx context.Context, stmt string, vs ...interface{}) cql.Cursor {
	return cursor{db.query(ctx, stmt, vs).Iter()}
}

type batch struct {
	*gocql.Batch

	db *DB
}

func (b batch) Exec() error {
	return b.db.sess.ExecuteBatch(b.Batch)
}

func (b batch) ExecCAS() (bool, cql.Cursor, error) {
	ok, iter, err := b.db.sess.ExecuteBatchCAS(b.Batch)

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

func (db *DB) Batch(ctx context.Context, bt cql.BatchType) cql.Batch {
	b := db.sess.NewBatch(gocqlBatchTypes[bt]).WithContext(ctx)

	return batch{Batch: b, db: db}
}
