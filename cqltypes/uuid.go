package cqltypes

import "github.com/gocql/gocql"

type UUID = gocql.UUID

var (
	ParseUUID    = gocql.ParseUUID
	RandomUUID   = gocql.RandomUUID
	UUIDFromTime = gocql.UUIDFromTime
	TimeUUID     = gocql.TimeUUID
)
