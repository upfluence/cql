module github.com/upfluence/cql

go 1.14

require (
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	github.com/golang/snappy v0.0.2
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.6.1
	github.com/upfluence/log v0.0.0-20200124211732-c9875854d3b8
	github.com/upfluence/pkg v1.8.4
	github.com/upfluence/sql v0.3.9
	gopkg.in/inf.v0 v0.9.1
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
