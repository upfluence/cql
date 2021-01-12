package cql

import (
	"context"
	"errors"
)

var ErrNoRows = errors.New("cql: No rows found")

type BatchType uint8

const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

type Option interface {
	IsCQLOption()
}

type Consistency uint16

const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	LocalOne    Consistency = 0x0A
)

type WithConsistency Consistency

func (WithConsistency) IsCQLOption() {}

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
