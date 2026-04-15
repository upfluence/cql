// Package cqlbuilder provides a type-safe query builder for constructing and executing CQL queries.
// It allows building INSERT, UPDATE, DELETE, and SELECT statements programmatically with
// support for named parameters, lightweight transactions (LWT), and batch operations.
package cqlbuilder

import "github.com/upfluence/cql"

// QueryBuilder wraps a cql.DB instance and provides methods to prepare typed statements.
// It serves as the entry point for building type-safe CQL queries with named parameters.
type QueryBuilder struct {
	cql.DB
}

func (qb *QueryBuilder) PrepareInsert(is InsertStatement) *InsertExecer {
	return &InsertExecer{
		execer:       execer{stmt: is, db: qb.DB},
		QueryBuilder: qb,
		Statement:    is,
	}
}

func (qb *QueryBuilder) PrepareDelete(ds DeleteStatement) *DeleteExecer {
	return &DeleteExecer{
		execer:       execer{stmt: ds, db: qb.DB},
		QueryBuilder: qb,
		Statement:    ds,
	}
}

func (qb *QueryBuilder) PrepareUpdate(us UpdateStatement) *UpdateExecer {
	return &UpdateExecer{
		execer:       execer{stmt: us, db: qb.DB},
		QueryBuilder: qb,
		Statement:    us,
	}
}

func (qb *QueryBuilder) PrepareSelect(ss SelectStatement) *SelectQueryer {
	return &SelectQueryer{QueryBuilder: qb, Statement: ss}
}

func (qb *QueryBuilder) PrepareBatch(bs BatchStatement) *BatchExecer {
	return &BatchExecer{QueryBuilder: qb, Statement: bs}
}

type statement interface {
	buildQuery(map[string]any) (string, []any, error)
}

// CASStatement represents a statement that supports compare-and-set operations.
// It extends the base statement interface with methods for handling lightweight transactions.
type CASStatement interface {
	statement

	casScanKeys() []string
}

// StaticCASStatement wraps a CASStatement with static attribute values.
// It allows executing a CAS statement with pre-defined parameter values.
type StaticCASStatement struct {
	CASStatement

	Attrs map[string]any
}

func (scs StaticCASStatement) buildQuery(map[string]any) (string, []any, error) {
	return scs.CASStatement.buildQuery(scs.Attrs)
}

// InsertExecer prepares and executes INSERT statements with support for DML options.
type InsertExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    InsertStatement
}

func (ie *InsertExecer) WithOptions(opts DMLOptions) Execer {
	stmt := ie.Statement
	stmt.Options = opts

	return ie.QueryBuilder.PrepareInsert(stmt)
}

// DeleteExecer prepares and executes DELETE statements with support for DML options.
type DeleteExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    DeleteStatement
}

func (de *DeleteExecer) WithOptions(opts DMLOptions) Execer {
	stmt := de.Statement
	stmt.Timestamp = opts.Timestamp

	return de.QueryBuilder.PrepareDelete(stmt)
}

// UpdateExecer prepares and executes UPDATE statements with support for DML options.
type UpdateExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    UpdateStatement
}

func (ue *UpdateExecer) WithOptions(opts DMLOptions) Execer {
	stmt := ue.Statement
	stmt.Options = opts

	return ue.QueryBuilder.PrepareUpdate(stmt)
}
