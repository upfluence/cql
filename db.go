package cql

import "context"

type BatchType uint8

const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

type Option interface {
	IsCQLOption()
}

type NamedQuery string

func (nq NamedQuery) IsCQLOption() {}

type CASScanner interface {
	ScanCAS(...interface{}) (bool, error)
}

type Scanner interface {
	Scan(...interface{}) error
}

type Cursor interface {
	Scan(...interface{}) bool
	Close() error
}

type Batch interface {
	Query(string, ...interface{})

	Exec() error
	ExecCAS() (bool, Cursor, error)
}

type DB interface {
	Exec(context.Context, string, ...interface{}) error
	ExecCAS(context.Context, string, ...interface{}) CASScanner

	QueryRow(context.Context, string, ...interface{}) Scanner
	Query(context.Context, string, ...interface{}) Cursor

	Batch(context.Context, BatchType) Batch
}

type MiddlewareFactory interface {
	Wrap(DB) DB
}
