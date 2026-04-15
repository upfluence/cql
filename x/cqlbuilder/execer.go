package cqlbuilder

import (
	"context"

	"github.com/upfluence/cql"
)

// CASScanner scans the results of a compare-and-set operation into a map of named values.
type CASScanner interface {
	ScanCAS(map[string]any) (bool, error)
}

type errCASScanner struct{ error }

func (ecs errCASScanner) ScanCAS(map[string]any) (bool, error) {
	return false, ecs.error
}

var zeroCASScanner = errCASScanner{}

type casScanner struct {
	sc cql.CASScanner
	ks []string
}

func (cs *casScanner) ScanCAS(qvs map[string]any) (bool, error) {
	vs := make([]any, len(cs.ks))

	for i, k := range cs.ks {
		v, ok := qvs[k]

		if !ok {
			return false, ErrMissingKey{Key: k}
		}

		vs[i] = v
	}

	return cs.sc.ScanCAS(vs...)
}

// Execer executes DML statements (INSERT, UPDATE, DELETE) with named parameters.
// It supports both regular execution and compare-and-set (lightweight transaction) execution.
type Execer interface {
	Exec(context.Context, map[string]any) error
	ExecCAS(context.Context, map[string]any) CASScanner

	WithOptions(DMLOptions) Execer
}

type execer struct {
	stmt CASStatement
	db   cql.DB
}

func (e *execer) Exec(ctx context.Context, qvs map[string]any) error {
	var stmt, vs, err = e.stmt.buildQuery(qvs)

	switch err {
	case nil:
	case skipClause:
		return nil
	default:
		return err
	}

	return e.db.Exec(ctx, stmt, vs...)
}

func (e *execer) ExecCAS(ctx context.Context, qvs map[string]any) CASScanner {
	var stmt, vs, err = e.stmt.buildQuery(qvs)

	switch err {
	case nil:
	case skipClause:
		return zeroCASScanner
	default:
		return errCASScanner{err}
	}

	return &casScanner{
		sc: e.db.ExecCAS(ctx, stmt, vs...),
		ks: e.stmt.casScanKeys(),
	}
}
