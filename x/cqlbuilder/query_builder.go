package cqlbuilder

import "github.com/upfluence/cql"

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
	buildQuery(map[string]interface{}) (string, []interface{}, error)
}

type CASStatement interface {
	statement

	casScanKeys() []string
}

type InsertExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    InsertStatement
}

type DeleteExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    DeleteStatement
}

type UpdateExecer struct {
	execer

	QueryBuilder *QueryBuilder
	Statement    UpdateStatement
}
