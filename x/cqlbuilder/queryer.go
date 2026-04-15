package cqlbuilder

import (
	"context"

	"github.com/upfluence/cql"
	"github.com/upfluence/errors"
)

// Queryer executes SELECT statements with named parameters and returns results as maps.
type Queryer interface {
	Query(context.Context, map[string]any) Cursor
	QueryRow(context.Context, map[string]any) Scanner
}

// Scanner scans a single row result into a map of named values.
type Scanner interface {
	Scan(map[string]any) error
}

type scanner struct {
	sc cql.Scanner
	ks []string
}

func (sc *scanner) Scan(vs map[string]any) error {
	var svs = make([]any, len(sc.ks))

	for i, k := range sc.ks {
		v, ok := vs[k]

		if !ok {
			return ErrMissingKey{Key: k}
		}

		svs[i] = v
	}

	return sc.sc.Scan(svs...)
}

type errScanner struct{ error }

func (es errScanner) Scan(map[string]any) error { return es.error }

var zeroScanner = errScanner{error: cql.ErrNoRows}

// Cursor iterates over multiple rows from a query result with named value scanning.
type Cursor interface {
	Scan(map[string]any) bool
	Close() error
}

type cursor struct {
	c  cql.Cursor
	ks []string

	err error
}

func (c *cursor) Scan(vs map[string]any) bool {
	var svs = make([]any, len(c.ks))

	for i, k := range c.ks {
		v, ok := vs[k]

		if !ok {
			c.err = ErrMissingKey{Key: k}
			return false
		}

		svs[i] = v
	}

	return c.c.Scan(svs...)
}

func (c *cursor) Close() error {
	return errors.Combine(c.err, c.c.Close())
}

type errCursor struct{ error }

func (ec errCursor) Scan(map[string]any) bool { return false }
func (ec errCursor) Close() error             { return ec.error }

var zeroCursor = errCursor{}
