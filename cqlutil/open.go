package cqlutil

import (
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/upfluence/pkg/cfg"

	"github.com/upfluence/cql"
	backend "github.com/upfluence/cql/backend/gocql"
)

var defaultbuilder = &builder{
	keyspace:        cfg.FetchString("CASSANDRA_KEYSPACE", "test"),
	cassandraURL:    cfg.FetchString("CASSANDRA_URL", "127.0.0.1"),
	port:            9042,
	protocolVersion: 3,
	consistency:     gocql.Quorum,
	timeout:         15 * time.Second,
	retryPolicy:     &gocql.SimpleRetryPolicy{NumRetries: 3},
}

func WithMiddleware(f cql.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

func Keyspace(k string) Option {
	return func(o *builder) { o.keyspace = k }
}

func CassandraURL(url string) Option {
	return func(o *builder) { o.cassandraURL = url }
}

func Consistency(c gocql.Consistency) Option {
	return func(o *builder) { o.consistency = c }
}

func Timeout(t time.Duration) Option {
	return func(o *builder) { o.timeout = t }
}

func Port(p int) Option {
	return func(o *builder) { o.port = p }
}

func RetryPolicy(p gocql.RetryPolicy) Option {
	return func(o *builder) { o.retryPolicy = p }
}

type builder struct {
	keyspace, cassandraURL string
	port, protocolVersion  int

	consistency gocql.Consistency
	retryPolicy gocql.RetryPolicy
	timeout     time.Duration

	middlewares []cql.MiddlewareFactory
}

func (o builder) cassandraIPs() []string {
	return strings.Split(o.cassandraURL, ",")
}

type Option func(*builder)

func Open(opts ...Option) (cql.DB, error) {
	opt := *defaultbuilder

	for _, optFn := range opts {
		optFn(&opt)
	}

	cluster := gocql.NewCluster(opt.cassandraIPs()...)

	cluster.Consistency = opt.consistency
	cluster.ProtoVersion = opt.protocolVersion
	cluster.Keyspace = opt.keyspace
	cluster.Timeout = opt.timeout
	cluster.RetryPolicy = opt.retryPolicy

	sess, err := cluster.CreateSession()

	if err != nil {
		return nil, err
	}

	var db cql.DB = backend.NewDB(sess)

	for _, m := range opt.middlewares {
		db = m.Wrap(db)
	}

	return db, nil
}
