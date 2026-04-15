package cqlbuilder

import (
	"io"
	"strings"
)

// QueryWriter extends io.Writer with the ability to add query arguments for prepared statements.
type QueryWriter interface {
	io.Writer

	AddArg(any)
}

type queryWriter struct {
	strings.Builder

	args []any
}

func (qw *queryWriter) AddArg(a any) { qw.args = append(qw.args, a) }
