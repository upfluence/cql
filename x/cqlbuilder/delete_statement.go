package cqlbuilder

import (
	"fmt"
	"strings"
	"time"
)

type LWTDeleteClause interface {
	LWTClause

	isDeleteClause()
}

type DeleteStatement struct {
	Table string

	Fields      []Marker
	WhereClause PredicateClause

	Timestamp time.Time
	LWTClause LWTDeleteClause
}

func (ds DeleteStatement) casScanKeys() []string {
	if lck, ok := ds.LWTClause.(interface{ keys() []string }); ok {
		return lck.keys()
	}

	return nil
}

func (ds DeleteStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
	var (
		qw queryWriter

		ks = make([]string, len(ds.Fields))
	)

	for i, f := range ds.Fields {
		k := f.ToCQL()

		if i == len(ds.Fields)-1 {
			k += " "
		}

		ks[i] = k
	}

	fmt.Fprintf(&qw, "DELETE %sFROM %s ", strings.Join(ks, ", "), ds.Table)

	DMLOptions{Timestamp: ds.Timestamp}.writeTo(&qw)

	qw.WriteString("WHERE ")

	if err := ds.WhereClause.WriteTo(&qw, qvs); err != nil {
		return "", nil, err
	}

	if lc := ds.LWTClause; lc != nil {
		qw.WriteRune(' ')

		if err := lc.writeTo(&qw, qvs); err != nil {
			return "", nil, err
		}
	}

	return qw.String(), qw.args, nil
}
