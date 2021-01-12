package cqlbuilder

import (
	"context"

	"github.com/upfluence/cql"
)

type CASScanner interface {
	ScanCAS(map[string]interface{}) (bool, error)
}

type errCASScanner struct{ error }

func (ecs errCASScanner) ScanCAS(map[string]interface{}) (bool, error) {
	return false, ecs.error
}

type casScanner struct {
	sc cql.CASScanner
	ks []string
}

func (cs *casScanner) ScanCAS(qvs map[string]interface{}) (bool, error) {
	vs := make([]interface{}, len(cs.ks))

	for i, k := range cs.ks {
		v, ok := qvs[k]

		if !ok {
			return false, ErrMissingKey{Key: k}
		}

		vs[i] = v
	}

	return cs.sc.ScanCAS(vs...)
}

type Execer interface {
	Exec(context.Context, map[string]interface{}) error
	ExecCAS(context.Context, map[string]interface{}) CASScanner
}

type execer struct {
	stmt CASStatement
	db   cql.DB
}

func (e *execer) Exec(ctx context.Context, qvs map[string]interface{}) error {
	var stmt, vs, err = e.stmt.buildQuery(qvs)

	if err != nil {
		return err
	}

	return e.db.Exec(ctx, stmt, vs...)
}

func (e *execer) ExecCAS(ctx context.Context, qvs map[string]interface{}) CASScanner {
	var stmt, vs, err = e.stmt.buildQuery(qvs)

	if err != nil {
		return errCASScanner{err}
	}

	return &casScanner{
		sc: e.db.ExecCAS(ctx, stmt, vs...),
		ks: e.stmt.casScanKeys(),
	}
}
