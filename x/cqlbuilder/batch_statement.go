package cqlbuilder

import (
	"context"

	"github.com/upfluence/cql"
)

type BatchStatement struct {
	Type cql.BatchType

	Statements []CASStatement
}

type BatchExecer struct {
	QueryBuilder *QueryBuilder
	Statement    BatchStatement
}

func (be *BatchExecer) Exec(ctx context.Context, qvs map[string]interface{}) error {
	var b = be.QueryBuilder.Batch(ctx, be.Statement.Type)

	for _, s := range be.Statement.Statements {
		stmt, vs, err := s.buildQuery(qvs)

		switch err {
		case nil:
			b.Query(stmt, vs...)
		case skipClause:
		default:
			return err
		}
	}

	return b.Exec()
}
