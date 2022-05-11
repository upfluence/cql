package cqlbuilder

import (
	"fmt"

	"github.com/upfluence/errors"
)

var errNoMarkers = errors.New("No marker given to the statement")

type ErrMissingKey struct{ Key string }

func (emk ErrMissingKey) Error() string {
	return fmt.Sprintf("%q key missing", emk.Key)
}

type Marker interface {
	Binding() string
	ToCQL() string
	Clone() Marker
}

func Column(k string) Marker { return column(k) }

type column string

func (c column) ColumnName() string { return string(c) }
func (c column) Binding() string    { return string(c) }
func (c column) ToCQL() string      { return string(c) }
func (c column) Clone() Marker      { return c }

func CQLExpression(m, exp string) Marker { return cqlMarker{m: m, cql: exp} }

type cqlMarker struct {
	m   string
	cql string
}

func (cm cqlMarker) Binding() string { return cm.m }
func (cm cqlMarker) ToCQL() string   { return cm.cql }
func (cm cqlMarker) Clone() Marker   { return cm }

func columnName(m Marker) string {
	if cn, ok := m.(interface{ ColumnName() string }); ok {
		return cn.ColumnName()
	}

	return m.ToCQL()
}
