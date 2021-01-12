package cqlbuilder

import (
	"io"
	"strings"
)

type QueryWriter interface {
	io.Writer

	AddArg(interface{})
}

type queryWriter struct {
	strings.Builder

	args []interface{}
}

func (qw *queryWriter) AddArg(a interface{}) { qw.args = append(qw.args, a) }
