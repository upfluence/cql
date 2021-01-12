package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqltest"
	"github.com/upfluence/cql/cqltypes"
	"github.com/upfluence/cql/x/migration"
)

func TestMigrationIntegration(t *testing.T) {
	cqltest.NewTestCase(
		cqltest.WithMigratorFunc(func(db cql.DB) migration.Migrator {
			return migration.NewMigrator(
				db,
				cqltest.StaticSource{
					MigrationUp:   "CREATE TABLE IF NOT EXISTS foo(uuid UUID PRIMARY KEY, data blob)",
					MigrationDown: "DROP TABLE foo",
				},
			)
		}),
	).Run(t, func(t *testing.T, db cql.DB) {
		uuid := cqltypes.TimeUUID()
		err := db.Exec(
			context.Background(),
			"INSERT INTO foo(uuid, data) VALUES (?, ?)",
			uuid,
			[]byte("foo"),
		)

		assert.NoError(t, err)

		var data []byte

		err = db.QueryRow(
			context.Background(),
			"SELECT data FROM foo WHERE uuid = ?",
			uuid,
		).Scan(&data)

		assert.NoError(t, err)
		assert.Equal(t, []byte("foo"), data)
	})
}
