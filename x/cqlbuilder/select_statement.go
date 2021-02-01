package cqlbuilder

import (
	"context"
	"fmt"
	"strings"
)

type Direction string

const (
	Asc  Direction = "ASC"
	Desc Direction = "DESC"
)

type OrderByClause struct {
	Field     Marker
	Direction Direction
}

type SelectStatement struct {
	Table string

	SelectClauses []Marker
	WhereClause   PredicateClause
	OrderByClause OrderByClause

	AllowFiltering bool
}

func (ss SelectStatement) scanKeys() []string {
	var vs = make([]string, len(ss.SelectClauses))

	for i, f := range ss.SelectClauses {
		vs[i] = f.Binding()
	}

	return vs
}

func (ss SelectStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
	var (
		qw queryWriter

		ks = make([]string, len(ss.SelectClauses))
	)

	for i, f := range ss.SelectClauses {
		ks[i] = f.ToCQL()
	}

	fmt.Fprintf(&qw, "SELECT %s FROM %s", strings.Join(ks, ", "), ss.Table)

	if ss.WhereClause != nil {
		qw.WriteString(" WHERE ")

		if err := ss.WhereClause.WriteTo(&qw, qvs); err != nil {
			return "", nil, err
		}
	}

	if obc := ss.OrderByClause; obc.Field != nil {
		fmt.Fprintf(&qw, " ORDER BY %s %s", obc.Field.ToCQL(), obc.Direction)
	}

	if ss.AllowFiltering {
		qw.WriteString(" ALLOW FILTERING")
	}

	return qw.String(), qw.args, nil
}

type SelectQueryer struct {
	QueryBuilder *QueryBuilder
	Statement    SelectStatement
}

func (sq *SelectQueryer) Query(ctx context.Context, qvs map[string]interface{}) Cursor {
	stmt, vs, err := sq.Statement.buildQuery(qvs)

	if err != nil {
		return errCursor{err}
	}

	return &cursor{
		c:  sq.QueryBuilder.Query(ctx, stmt, vs...),
		ks: sq.Statement.scanKeys(),
	}
}

func (sq *SelectQueryer) QueryRow(ctx context.Context, qvs map[string]interface{}) Scanner {
	stmt, vs, err := sq.Statement.buildQuery(qvs)

	if err != nil {
		return errScanner{err}
	}

	return &scanner{
		sc: sq.QueryBuilder.QueryRow(ctx, stmt, vs...),
		ks: sq.Statement.scanKeys(),
	}
}
