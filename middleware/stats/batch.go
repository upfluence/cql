package stats

import "github.com/upfluence/cql"

type batch struct {
	cql.Batch

	instruments instruments
	queries     int64
}

func (b *batch) Query(stmt string, vs ...any) {
	b.queries++
	b.Batch.Query(stmt, vs...)
}

func (b *batch) Exec() error {
	b.instruments.batchQueries.Add(b.queries)

	operation := b.instruments.batchExec.Begin()
	err := b.Batch.Exec()
	operation.Finish(err)

	return err //nolint:wrapcheck
}

func (b *batch) ExecCAS() (bool, cql.Cursor, error) {
	b.instruments.batchQueries.Add(b.queries)

	operation := b.instruments.batchExecCAS.Begin()
	applied, cur, err := b.Batch.ExecCAS()
	operation.Finish(err)

	return applied, cur, err //nolint:wrapcheck
}
