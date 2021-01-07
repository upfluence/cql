package cqltest

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/upfluence/log/record"
	"github.com/upfluence/pkg/cfg"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/cqlutil"
	"github.com/upfluence/cql/middleware/logger"
	"github.com/upfluence/cql/x/migration"
)

const createKeyspaceStmtFmt = "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"

type testLogger struct {
	testing.TB
}

func (tl testLogger) Log(ot logger.OpType, q string, vs []interface{}, _ error, d time.Duration, fs ...record.Field) {
	var b strings.Builder

	fmt.Fprintf(&b, "[OpType: %s] [Duration: %s] ", ot, d.String())

	for _, f := range fs {
		fmt.Fprintf(&b, "[%s: %s]", f.GetKey(), f.GetValue())
	}

	for i, v := range vs {
		fmt.Fprintf(&b, "[$%d: %v] ", i+1, v)
	}

	b.WriteString(q)

	tl.TB.Log(b.String())
}

type TestCase struct {
	ip       func() string
	keyspace func() string

	opts []cqlutil.Option
	mfns []func(cql.DB) migration.Migrator
}

func envFunc(env, other string) func() string {
	return func() string { return cfg.FetchString(env, other) }
}

type TestCaseOption func(*TestCase)

func WithMigratorFunc(fn func(cql.DB) migration.Migrator) TestCaseOption {
	return func(tc *TestCase) { tc.mfns = append(tc.mfns, fn) }
}

func NewTestCase(opts ...TestCaseOption) *TestCase {
	var tc = TestCase{
		ip:       envFunc("CASSANDRA_IP", "127.0.0.1"),
		keyspace: envFunc("CASSANDRA_KEYSPACE", ""),
		opts:     []cqlutil.Option{cqlutil.NoGossip},
	}

	for _, opt := range opts {
		opt(&tc)
	}

	return &tc
}

func (tc *TestCase) buildDB(t *testing.T, keyspace string) cql.DB {
	db, err := cqlutil.Open(
		append(
			tc.opts,
			cqlutil.CassandraURL(tc.ip()),
			cqlutil.Keyspace(keyspace),
			cqlutil.WithMiddleware(logger.NewFactory(testLogger{t})),
		)...,
	)

	if err != nil {
		t.Fatalf("Cannot build cassandra DB: %+v", err)
	}

	return db
}

func (tc *TestCase) Run(t *testing.T, fn func(t *testing.T, db cql.DB)) {
	t.Helper()

	keyspace := tc.keyspace()

	if keyspace == "" {
		t.Skip("No cassandra keyspace given, skipping test case")
	}

	db := tc.buildDB(t, "system")

	if err := db.Exec(
		context.Background(),
		fmt.Sprintf(createKeyspaceStmtFmt, keyspace),
	); err != nil {
		t.Fatalf("Cannot create testing keyspace: %+v", err)
	}

	db = tc.buildDB(t, keyspace)

	for _, mfn := range tc.mfns {
		if err := mfn(db).Up(context.Background()); err != nil {
			t.Fatalf("can not proceed the migration up: %v", err.Error())
		}
	}

	fn(t, db)

	for _, mfn := range tc.mfns {
		if err := mfn(db).Down(context.Background()); err != nil {
			t.Fatalf("can not proceed the migration up: %v", err.Error())
		}
	}
}
