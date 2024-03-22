package cqlbuilder

import (
	"fmt"
	"strings"

	"github.com/upfluence/cql"
)

type LWTInsertClause interface {
	LWTClause

	isInsertClause()
}

type InsertStatement struct {
	Table string

	Fields []Marker

	Options     DMLOptions
	LWTClause   LWTInsertClause
	Consistency cql.Consistency
}

func (is InsertStatement) casScanKeys() []string {
	var ks = make([]string, len(is.Fields))

	for i, f := range is.Fields {
		ks[i] = f.Binding()
	}

	return ks
}

func (is InsertStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
	var (
		qw queryWriter

		ks = make([]string, len(is.Fields))
		qs = make([]string, len(is.Fields))
	)

	if len(is.Fields) == 0 {
		return "", nil, errNoMarkers
	}

	for i, f := range is.Fields {
		k := f.Binding()
		v, ok := qvs[k]

		if !ok {
			return "", nil, ErrMissingKey{Key: k}
		}

		ks[i] = columnName(f)
		qs[i] = "?"
		qw.AddArg(v)
	}

	fmt.Fprintf(
		&qw,
		"INSERT INTO %s(%s) VALUES (%s)",
		is.Table,
		strings.Join(ks, ", "),
		strings.Join(qs, ", "),
	)

	if lc := is.LWTClause; lc != nil {
		qw.WriteRune(' ')

		if err := lc.writeTo(&qw, qvs); err != nil {
			return "", nil, err
		}
	}

	is.Options.writeTo(&qw)

	if is.Consistency > cql.Any {
		qw.args = append(qw.args, cql.WithConsistency(is.Consistency))
	}

	return qw.String(), qw.args, nil
}
