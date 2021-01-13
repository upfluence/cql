package cqlbuilder

import (
	"errors"
	"fmt"
	"io"
	"reflect"
)

var errInvalidType = errors.New("x/cqlbuilder: invalid type")

type PredicateClause interface {
	WriteTo(QueryWriter, map[string]interface{}) error
	Clone() PredicateClause
	Markers() []Marker
}

func Eq(m Marker) PredicateClause   { return signClause(m, "=") }
func Ne(m Marker) PredicateClause   { return signClause(m, "!=") }
func Lt(m Marker) PredicateClause   { return signClause(m, "<") }
func Lte(m Marker) PredicateClause  { return signClause(m, "<=") }
func Gt(m Marker) PredicateClause   { return signClause(m, ">") }
func Gte(m Marker) PredicateClause  { return signClause(m, ">=") }
func Like(m Marker) PredicateClause { return signClause(m, "LIKE") }

func signClause(m Marker, s string) *basicClause {
	return &basicClause{m: m, fn: writeSignClause(s)}
}

func writeSignClause(s string) func(QueryWriter, interface{}, string) error {
	return func(qw QueryWriter, vv interface{}, k string) error {
		fmt.Fprintf(qw, "%s %s ?", k, s)
		qw.AddArg(vv)
		return nil
	}
}

func In(m Marker) PredicateClause {
	return &basicClause{m: m, fn: writeInClause}
}

type basicClause struct {
	m  Marker
	fn func(QueryWriter, interface{}, string) error
}

func (bc *basicClause) Markers() []Marker { return []Marker{bc.m} }

func (bc *basicClause) Clone() PredicateClause {
	return &basicClause{m: bc.m.Clone(), fn: bc.fn}
}

func (bc *basicClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	b := bc.m.Binding()
	vv, ok := vs[b]

	if !ok {
		return ErrMissingKey{b}
	}

	return bc.fn(w, vv, bc.m.ToCQL())
}

func parseItems(vv interface{}) ([]interface{}, error) {
	var v = reflect.ValueOf(vv)

	if k := v.Kind(); k != reflect.Slice && k != reflect.Array {
		return nil, errInvalidType
	}

	res := make([]interface{}, v.Len())

	for i := 0; i < v.Len(); i++ {
		res[i] = v.Index(i).Interface()
	}

	return res, nil
}

func writeInClause(qw QueryWriter, vv interface{}, k string) error {
	vs, err := parseItems(vv)

	if err != nil {
		return err
	}

	if len(vs) == 0 {
		io.WriteString(qw, "1=0")
		return nil
	}

	fmt.Fprintf(qw, "%s IN (", k)

	for i, v := range vs {
		io.WriteString(qw, "?")
		qw.AddArg(v)

		if i < len(vs)-1 {
			io.WriteString(qw, ", ")
		}
	}

	io.WriteString(qw, ")")
	return nil
}

func Static(pc PredicateClause, vs map[string]interface{}) PredicateClause {
	return &staticValuePredicateClauseWrapper{
		svpc: &staticClause{pc: pc, vs: vs},
	}
}

func StaticEq(m Marker, v interface{}) PredicateClause {
	return Static(Eq(m), map[string]interface{}{m.Binding(): v})
}

type staticValuePredicateClauseWrapper struct {
	svpc StaticValuePredicateClause
}

func (svpcw *staticValuePredicateClauseWrapper) Markers() []Marker {
	return svpcw.svpc.Markers()
}

func (svpcw *staticValuePredicateClauseWrapper) Clone() PredicateClause {
	return &staticValuePredicateClauseWrapper{
		svpc: svpcw.svpc.Clone(),
	}
}

func (svpcw *staticValuePredicateClauseWrapper) WriteTo(w QueryWriter, _ map[string]interface{}) error {
	return svpcw.svpc.WriteTo(w)
}

type staticClause struct {
	pc PredicateClause
	vs map[string]interface{}
}

func (sc *staticClause) Clone() StaticValuePredicateClause {
	vs := make(map[string]interface{}, len(sc.vs))

	for k, v := range sc.vs {
		vs[k] = v
	}

	return &staticClause{pc: sc.pc.Clone(), vs: vs}
}

func (sc *staticClause) WriteTo(w QueryWriter) error {
	return sc.pc.WriteTo(w, sc.vs)
}

func (sc *staticClause) Markers() []Marker {
	return sc.pc.Markers()
}

type StaticValuePredicateClause interface {
	WriteTo(QueryWriter) error
	Clone() StaticValuePredicateClause
	Markers() []Marker
}
