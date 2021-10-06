package cqlbuilder

import (
	"context"

	"github.com/upfluence/cql"
	"github.com/upfluence/pkg/multierror"
)

type Queryer interface {
	Query(context.Context, map[string]interface{}) Cursor
	QueryRow(context.Context, map[string]interface{}) Scanner
}

type Scanner interface {
	Scan(map[string]interface{}) error
}

type scanner struct {
	sc cql.Scanner
	ks []string
}

func (sc *scanner) Scan(vs map[string]interface{}) error {
	var svs = make([]interface{}, len(sc.ks))

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

func (es errScanner) Scan(map[string]interface{}) error { return es.error }

type Cursor interface {
	Scan(map[string]interface{}) bool
	Close() error
}

type cursor struct {
	c  cql.Cursor
	ks []string

	err error
}

func (c *cursor) Scan(vs map[string]interface{}) bool {
	var svs = make([]interface{}, len(c.ks))

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
	return multierror.Combine(c.err, c.c.Close())
}

type errCursor struct{ error }

func (ec errCursor) Scan(map[string]interface{}) bool { return false }
func (ec errCursor) Close() error                     { return ec.error }
