// Package cql provides a database-agnostic interface for interacting with Apache Cassandra.
// It defines core abstractions for executing queries, managing batches, and handling
// consistency levels. The package supports both regular queries and lightweight transactions
// (compare-and-set operations).
package cql

import (
	"context"

	"github.com/upfluence/errors"
)

var ErrNoRows = errors.New("no rows found")

// BatchType represents the type of batch operation to perform in Cassandra.
// Batches allow multiple mutations to be executed atomically.
//
//go:generate stringer -type=BatchType
type BatchType uint8

const (
	LoggedBatch BatchType = iota
	UnloggedBatch
	CounterBatch
)

// Option represents a query or batch configuration option.
// Implementations can modify query behavior such as consistency levels.
type Option interface {
	IsCQLOption()
}

// Consistency defines the consistency level for read and write operations.
// It determines how many replicas must respond before acknowledging an operation.
//
//go:generate stringer -type=Consistency
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

// WithConsistency is an Option that specifies the consistency level for a query or batch.
type WithConsistency Consistency

func (WithConsistency) IsCQLOption() {}

// NamedQuery is an Option that associates a name with a query for tracing and monitoring.
type NamedQuery string

func (nq NamedQuery) IsCQLOption() {}

// CASScanner provides the ability to scan results from a compare-and-set (lightweight transaction) operation.
// The ScanCAS method returns whether the operation was applied and any error encountered.
type CASScanner interface {
	ScanCAS(...any) (bool, error)
}

// Scanner provides the ability to scan a single row result into destination variables.
type Scanner interface {
	Scan(...any) error
}

// Cursor represents an iterator over multiple rows returned by a query.
// It allows scanning rows one at a time and must be closed when done.
type Cursor interface {
	Scan(...any) bool
	Close() error
}

// Batch represents a batch of queries that can be executed atomically.
// Multiple Query calls can be added to the batch before executing with Exec or ExecCAS.
type Batch interface {
	Query(string, ...any)

	Exec() error
	ExecCAS() (bool, Cursor, error)
}

// DB is the main interface for executing CQL queries against Cassandra.
// It provides methods for executing statements, querying rows, and creating batches.
// All operations accept a context for cancellation and deadline support.
type DB interface {
	Exec(context.Context, string, ...any) error
	ExecCAS(context.Context, string, ...any) CASScanner

	QueryRow(context.Context, string, ...any) Scanner
	Query(context.Context, string, ...any) Cursor

	Batch(context.Context, BatchType, ...Option) Batch
}

// MiddlewareFactory wraps a DB instance with additional functionality such as logging or metrics.
// Implementations can intercept and modify database operations.
type MiddlewareFactory interface {
	Wrap(DB) DB
}
