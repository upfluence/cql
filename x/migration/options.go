package migration

import (
	"fmt"

	"github.com/upfluence/pkg/timeutil"

	"github.com/upfluence/cql"
)

const (
	createTableMigrationStmtTmpl = `
CREATE TABLE IF NOT EXISTS %s (
	num int,
	dirty boolean,
	created_at timestamp,
	PRIMARY KEY (num)
)
	`

	fetchMigrationsStmtTmpl = `SELECT num, dirty FROM %s`
	createMigrationStmtTmpl = `INSERT INTO %s(num, dirty, created_at) VALUES(?, true, ?) IF NOT EXISTS`
	updateMigrationStmtTmpl = `UPDATE %s SET dirty = ? WHERE num = ? IF dirty = ?`
	deleteMigrationStmtTmpl = `DELETE FROM %s WHERE num = ? IF EXISTS`
)

var defaultOptions = options{
	table:       "migrations",
	consistency: cql.Quorum,
	clock:       timeutil.Background(),
}

type Option func(*options)

type options struct {
	table string

	consistency cql.Consistency
	clock       timeutil.Clock
}

func (o *options) createTableMigrationStmt() string {
	return fmt.Sprintf(createTableMigrationStmtTmpl, o.table)
}

func (o *options) fetchMigrationsStmt() string {
	return fmt.Sprintf(fetchMigrationsStmtTmpl, o.table)
}

func (o *options) createMigrationStmt() string {
	return fmt.Sprintf(createMigrationStmtTmpl, o.table)
}

func (o *options) updateMigrationStmt() string {
	return fmt.Sprintf(updateMigrationStmtTmpl, o.table)
}
func (o *options) deleteMigrationStmt() string {
	return fmt.Sprintf(deleteMigrationStmtTmpl, o.table)
}
