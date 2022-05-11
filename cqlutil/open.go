package cqlutil

import (
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/upfluence/cql"
	backend "github.com/upfluence/cql/backend/gocql"
)

var NoGossip = WithCQLOption(
	func(cc *gocql.ClusterConfig) {
		cc.DisableInitialHostLookup = true
		cc.IgnorePeerAddr = true
	},
)

func WithCQLOption(fn func(*gocql.ClusterConfig)) Option {
	return func(o *builder) { o.cqlOptions = append(o.cqlOptions, fn) }
}

func WithMiddleware(f cql.MiddlewareFactory) Option {
	return func(b *builder) { b.middlewares = append(b.middlewares, f) }
}

func Keyspace(k string) Option {
	return WithCQLOption(func(cc *gocql.ClusterConfig) { cc.Keyspace = k })
}

func CassandraURL(url string) Option {
	return func(o *builder) { o.cassandraURL = url }
}

func Consistency(c gocql.Consistency) Option {
	return WithCQLOption(func(cc *gocql.ClusterConfig) { cc.Consistency = c })
}

func Timeout(t time.Duration) Option {
	return WithCQLOption(func(cc *gocql.ClusterConfig) { cc.Timeout = t })
}

func Port(p int) Option {
	return WithCQLOption(func(cc *gocql.ClusterConfig) { cc.Port = p })
}

func RetryPolicy(p gocql.RetryPolicy) Option {
	return WithCQLOption(func(cc *gocql.ClusterConfig) { cc.RetryPolicy = p })
}

type builder struct {
	cassandraURL string

	cqlOptions  []func(*gocql.ClusterConfig)
	middlewares []cql.MiddlewareFactory
}

func (b *builder) clusterConfig() *gocql.ClusterConfig {
	cc := gocql.NewCluster(strings.Split(b.cassandraURL, ",")...)

	for _, opt := range b.cqlOptions {
		opt(cc)
	}

	return cc
}

func fetchString(env, fallback string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}

	return fallback
}

type Option func(*builder)

func Open(opts ...Option) (cql.DB, error) {
	b := builder{
		cassandraURL: fetchString("CASSANDRA_URL", "127.0.0.1"),
		cqlOptions: []func(*gocql.ClusterConfig){
			func(cc *gocql.ClusterConfig) {
				cc.Keyspace = fetchString("CASSANDRA_KEYSPACE", "test")
				cc.ProtoVersion = 3
				cc.Consistency = gocql.Quorum
				cc.Timeout = 15 * time.Second
				cc.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
			},
		},
	}

	for _, opt := range opts {
		opt(&b)
	}

	sess, err := b.clusterConfig().CreateSession()

	if err != nil {
		return nil, err
	}

	var db cql.DB = backend.NewDB(sess)

	for _, m := range b.middlewares {
		db = m.Wrap(db)
	}

	return db, nil
}
