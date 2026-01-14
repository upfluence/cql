package cqltypes

import gocql "github.com/apache/cassandra-gocql-driver/v2"

type UUID = gocql.UUID

var (
	ParseUUID    = gocql.ParseUUID
	RandomUUID   = gocql.RandomUUID
	UUIDFromTime = gocql.UUIDFromTime
	TimeUUID     = gocql.TimeUUID
)
