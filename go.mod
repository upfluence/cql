module github.com/upfluence/cql

go 1.16

require (
	github.com/gocql/gocql v0.0.0-20201215165327-e49edf966d90
	github.com/golang/snappy v0.0.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.6.0 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/upfluence/log v0.0.0-20200124211732-c9875854d3b8
	github.com/upfluence/pkg v1.8.4
	golang.org/x/sys v0.0.0-20200323222414-85ca7c5b95cd // indirect
)

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
