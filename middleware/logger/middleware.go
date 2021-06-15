package logger

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"

	"github.com/upfluence/cql"
)

type OpType string

const (
	Exec     OpType = "Exec"
	ExecCAS  OpType = "ExecCAS"
	QueryRow OpType = "QueryRow"
	Query    OpType = "Query"
)

type Logger interface {
	Log(OpType, string, []interface{}, error, time.Duration, ...record.Field)
}

type simplifiedLogger struct {
	level  record.Level
	logger log.Logger
}

func (l *simplifiedLogger) Log(t OpType, q string, vs []interface{}, err error, d time.Duration, ofs ...record.Field) {
	var (
		i int

		fs = make([]record.Field, 0, 2+len(ofs))
	)

	fs = append(fs, log.Field("op type", string(t)))
	fs = append(fs, log.Field("duration", d))

	for _, v := range vs {
		if _, ok := v.(cql.Option); ok {
			continue
		}

		fs = append(fs, log.Field(fmt.Sprintf("$%d", i+1), v))
		i++
	}

	fs = append(fs, ofs...)

	logger := l.logger

	if err != nil {
		logger = logger.WithError(err)
	}

	logger.WithFields(fs...).Log(l.level, q)
}

func NewFactory(l Logger) cql.MiddlewareFactory {
	return &factory{l: l}
}

func NewLevelFactory(l log.Logger, lvl record.Level) cql.MiddlewareFactory {
	return NewFactory(&simplifiedLogger{logger: l, level: lvl})
}

func NewDebugFactory(l log.Logger) cql.MiddlewareFactory {
	return NewLevelFactory(l, record.Debug)
}

type factory struct {
	l Logger
}

func (f *factory) Wrap(db cql.DB) cql.DB {
	return &DB{db: db, l: f.l}
}

type DB struct {
	db cql.DB
	l  Logger
}

func trimValues(vs []interface{}) ([]interface{}, []record.Field) {
	var fs []record.Field

	for _, v := range vs {
		if nq, ok := v.(cql.NamedQuery); ok {
			fs = append(fs, log.Field("query", string(nq)))
		}
	}

	return vs, fs
}

func (db *DB) Exec(ctx context.Context, stmt string, vs ...interface{}) error {
	t0 := time.Now()
	err := db.db.Exec(ctx, stmt, vs...)

	vs, fs := trimValues(vs)
	db.l.Log(Exec, stmt, vs, err, time.Since(t0), fs...)

	return err
}

type casScanner struct {
	cql.CASScanner

	l    Logger
	stmt string
	vs   []interface{}
	t0   time.Time
}

func (csc casScanner) ScanCAS(vs ...interface{}) (bool, error) {
	ok, err := csc.CASScanner.ScanCAS(vs...)

	vvs, fs := trimValues(csc.vs)

	csc.l.Log(
		ExecCAS,
		csc.stmt,
		vvs,
		err,
		time.Since(csc.t0),
		append(fs, log.Field("applied", ok))...,
	)

	return ok, err
}

func (db *DB) ExecCAS(ctx context.Context, stmt string, vs ...interface{}) cql.CASScanner {
	t0 := time.Now()

	sc := db.db.ExecCAS(ctx, stmt, vs...)

	return casScanner{CASScanner: sc, l: db.l, stmt: stmt, vs: vs, t0: t0}
}

type scanner struct {
	cql.Scanner

	l    Logger
	stmt string
	vs   []interface{}
	t0   time.Time
}

func (sc scanner) Scan(vs ...interface{}) error {
	err := sc.Scanner.Scan(vs...)

	vvs, fs := trimValues(sc.vs)

	sc.l.Log(
		QueryRow,
		sc.stmt,
		vvs,
		err,
		time.Since(sc.t0),
		fs...,
	)

	return err
}

func (db *DB) QueryRow(ctx context.Context, stmt string, vs ...interface{}) cql.Scanner {
	t0 := time.Now()

	sc := db.db.QueryRow(ctx, stmt, vs...)

	return scanner{Scanner: sc, l: db.l, stmt: stmt, vs: vs, t0: t0}
}

type cursor struct {
	cql.Cursor

	l    Logger
	stmt string
	vs   []interface{}
	t0   time.Time

	scanned uint32
}

func (c *cursor) Scan(vs ...interface{}) bool {
	atomic.AddUint32(&c.scanned, 1)

	return c.Cursor.Scan(vs...)
}

func (c *cursor) Close() error {
	err := c.Cursor.Close()
	vvs, fs := trimValues(c.vs)

	c.l.Log(
		Query,
		c.stmt,
		vvs,
		err,
		time.Since(c.t0),
		append(fs, log.Field("scanned", int64(c.scanned)))...,
	)

	return err
}

func (db *DB) Query(ctx context.Context, stmt string, vs ...interface{}) cql.Cursor {
	t0 := time.Now()

	c := db.db.Query(ctx, stmt, vs...)

	return &cursor{Cursor: c, l: db.l, stmt: stmt, vs: vs, t0: t0}
}

type batch struct {
	cql.Batch

	l Logger

	bt      cql.BatchType
	queries uint32
}

func (b *batch) Query(stmt string, vs ...interface{}) {
	atomic.AddUint32(&b.queries, 1)

	b.Batch.Query(stmt, vs...)
}

func (b *batch) Exec() error {
	t0 := time.Now()
	err := b.Batch.Exec()

	b.l.Log(
		Exec,
		"",
		nil,
		err,
		time.Since(t0),
		log.Field("queries", int64(b.queries)),
		log.Field("batch_type", b.bt),
	)

	return err
}

func (b *batch) ExecCAS() (bool, cql.Cursor, error) {
	t0 := time.Now()
	ok, cur, err := b.Batch.ExecCAS()

	b.l.Log(
		Exec,
		"",
		nil,
		err,
		time.Since(t0),
		log.Field("applied", ok),
		log.Field("queries", int64(b.queries)),
		log.Field("batch_type", b.bt),
	)

	return ok, cur, err
}

func (db *DB) Batch(ctx context.Context, bt cql.BatchType) cql.Batch {
	return &batch{Batch: db.db.Batch(ctx, bt), l: db.l, bt: bt}
}
