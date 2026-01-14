package gocql_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqltest"
	"github.com/upfluence/cql/cqltypes"
	"github.com/upfluence/cql/x/migration"
)

func TestIntegration(t *testing.T) {
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

		b := db.Batch(context.Background(), cql.LoggedBatch)

		uuid2 := cqltypes.TimeUUID()
		b.Query("INSERT INTO foo(uuid, data) VALUES (?, ?)", uuid2, []byte("bar"))
		b.Query("UPDATE foo SET data = ? WHERE uuid = ?", []byte("baz"), uuid)

		err = b.Exec()
		assert.NoError(t, err)

		ok, err := db.ExecCAS(
			context.Background(),
			"UPDATE foo SET data = ? WHERE uuid = ? IF data = ?",
			[]byte("qux"),
			uuid,
			[]byte("baz"),
		).ScanCAS()

		assert.NoError(t, err)
		assert.True(t, ok)

		cur := db.Query(
			context.Background(),
			"SELECT uuid, data FROM foo",
		)

		res := make(map[string][]byte)

		var id cqltypes.UUID

		for cur.Scan(&id, &data) {
			res[id.String()] = bytes.Clone(data)
		}

		assert.NoError(t, cur.Close())
		assert.Equal(t, map[string][]byte{
			uuid.String():  []byte("qux"),
			uuid2.String(): []byte("bar"),
		}, res)
	})
}
