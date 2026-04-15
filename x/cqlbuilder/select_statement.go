package cqlbuilder

import (
	"context"
	"fmt"
	"strings"

	"github.com/upfluence/cql"
)

type Direction string

const (
	Asc  Direction = "ASC"
	Desc Direction = "DESC"
)

// OrderByClause specifies the ordering of query results by a field and direction.
type OrderByClause struct {
	Field     Marker
	Direction Direction
}

// NullableInt represents an optional integer value for SQL LIMIT clauses.
type NullableInt struct {
	Int   int
	Valid bool
}

// SelectStatement represents a CQL SELECT query with WHERE, ORDER BY, and LIMIT clauses.
type SelectStatement struct {
	Table string

	SelectClauses []Marker
	WhereClause   PredicateClause
	OrderByClause OrderByClause

	Limit NullableInt

	Consistency    cql.Consistency
	AllowFiltering bool
}

func (ss SelectStatement) scanKeys() []string {
	var vs = make([]string, len(ss.SelectClauses))

	for i, f := range ss.SelectClauses {
		vs[i] = f.Binding()
	}

	return vs
}

func (ss SelectStatement) buildQuery(qvs map[string]any) (string, []any, error) {
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

	if ss.Limit.Valid {
		fmt.Fprintf(&qw, " LIMIT %d", ss.Limit.Int)
	}

	if ss.AllowFiltering {
		qw.WriteString(" ALLOW FILTERING")
	}

	if ss.Consistency > cql.Any {
		qw.args = append(qw.args, cql.WithConsistency(ss.Consistency))
	}

	return qw.String(), qw.args, nil
}

// SelectQueryer prepares and executes SELECT statements with named parameter support.
type SelectQueryer struct {
	QueryBuilder *QueryBuilder
	Statement    SelectStatement
}

func (sq *SelectQueryer) Query(ctx context.Context, qvs map[string]any) Cursor {
	stmt, vs, err := sq.Statement.buildQuery(qvs)

	switch err {
	case nil:
	case skipClause:
		return zeroCursor
	default:
		return errCursor{err}
	}

	return &cursor{
		c:  sq.QueryBuilder.Query(ctx, stmt, vs...),
		ks: sq.Statement.scanKeys(),
	}
}

func (sq *SelectQueryer) QueryRow(ctx context.Context, qvs map[string]any) Scanner {
	stmt, vs, err := sq.Statement.buildQuery(qvs)

	switch err {
	case nil:
	case skipClause:
		return zeroScanner
	default:
		return errScanner{err}
	}

	return &scanner{
		sc: sq.QueryBuilder.QueryRow(ctx, stmt, vs...),
		ks: sq.Statement.scanKeys(),
	}
}
