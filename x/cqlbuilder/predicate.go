package cqlbuilder

import (
	"fmt"
	"io"
	"reflect"

	"github.com/upfluence/errors"
)

var (
	errInvalidType = errors.New("invalid type")
	skipClause     = errors.New("skip clause")
)

type PredicateClause interface {
	WriteTo(QueryWriter, map[string]interface{}) error
	Clone() PredicateClause
	Markers() []Marker
}

type plainCQLPredicate string

func PlainCQLPredicate(stmt string) PredicateClause {
	return plainCQLPredicate(stmt)
}

func (pcp plainCQLPredicate) WriteTo(qw QueryWriter, _ map[string]interface{}) error {
	_, err := io.WriteString(qw, string(pcp))
	return err
}

func (pcp plainCQLPredicate) Clone() PredicateClause { return pcp }
func (pcp plainCQLPredicate) Markers() []Marker      { return nil }

func Eq(m Marker) PredicateClause  { return signClause(m, "=") }
func Ne(m Marker) PredicateClause  { return signClause(m, "!=") }
func Lt(m Marker) PredicateClause  { return signClause(m, "<") }
func Lte(m Marker) PredicateClause { return signClause(m, "<=") }
func Gt(m Marker) PredicateClause  { return signClause(m, ">") }
func Gte(m Marker) PredicateClause { return signClause(m, ">=") }

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

func StaticIn(m Marker, v interface{}) PredicateClause {
	return Static(In(m), map[string]interface{}{m.Binding(): v})
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
		return skipClause
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

type compoundedInClause struct {
	k  string
	ks []Marker
}

func (cic *compoundedInClause) Clone() PredicateClause {
	return &compoundedInClause{
		k:  cic.k,
		ks: append([]Marker{}, cic.ks...),
	}
}

func (cic *compoundedInClause) Markers() []Marker { return cic.ks }

func (cic *compoundedInClause) WriteTo(qw QueryWriter, avs map[string]interface{}) error {
	vs, ok := avs[cic.k].([]map[string]interface{})

	if !ok {
		return fmt.Errorf(
			"values for the compounded in %q are missing, or not formatted as %T",
			cic.k,
			vs,
		)
	}

	if len(vs) == 0 {
		return skipClause
	}

	io.WriteString(qw, "(")

	for i, k := range cic.ks {
		io.WriteString(qw, k.ToCQL())

		if i < len(cic.ks)-1 {
			io.WriteString(qw, ", ")
		}
	}

	io.WriteString(qw, ") IN (")

	for i, v := range vs {
		io.WriteString(qw, "(")

		for j, k := range cic.ks {
			kv, ok := v[k.Binding()]

			if !ok {
				return fmt.Errorf("Compounded in missing value for key %q", k)
			}

			io.WriteString(qw, "?")

			if j < len(cic.ks)-1 {
				io.WriteString(qw, ", ")
			}

			qw.AddArg(kv)
		}

		io.WriteString(qw, ")")

		if i < len(vs)-1 {
			io.WriteString(qw, ", ")
		}
	}

	io.WriteString(qw, ")")
	return nil
}

func CompoundedIn(k string, ks []Marker) PredicateClause {
	return &compoundedInClause{k: k, ks: ks}
}

func StaticCompoundedIn(ks []Marker, vs []map[string]interface{}) PredicateClause {
	return Static(
		CompoundedIn("values", ks),
		map[string]interface{}{"values": vs},
	)
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

type multiClause struct {
	wcs []PredicateClause

	op string
}

func wrapMultiClause(wcs []PredicateClause, op string) PredicateClause {
	var cs []PredicateClause

	for _, wc := range wcs {
		if wc == nil {
			continue
		}

		if mc, ok := wc.(multiClause); ok && mc.op == op {
			cs = append(cs, mc.wcs...)
			continue
		}

		cs = append(cs, wc)
	}

	switch len(cs) {
	case 0:
		return nil
	case 1:
		return cs[0]
	default:
		return multiClause{wcs: cs, op: op}
	}
}

func And(wcs ...PredicateClause) PredicateClause {
	return wrapMultiClause(wcs, "AND")
}

func (mc multiClause) Markers() []Marker {
	var ms []Marker

	for _, c := range mc.wcs {
		ms = append(ms, c.Markers()...)
	}

	return ms
}

func (mc multiClause) Clone() PredicateClause {
	var wcs []PredicateClause

	if len(mc.wcs) > 0 {
		wcs = make([]PredicateClause, len(mc.wcs))

		for i, pc := range mc.wcs {
			wcs[i] = pc.Clone()
		}
	}

	return multiClause{wcs: wcs, op: mc.op}
}

func (mc multiClause) WriteTo(w QueryWriter, vs map[string]interface{}) error {
	if len(mc.wcs) == 0 {
		return skipClause
	}

	io.WriteString(w, "(")

	for i, wc := range mc.wcs {
		if err := wc.WriteTo(w, vs); err != nil {
			return err
		}

		if i < len(mc.wcs)-1 {
			fmt.Fprintf(w, ") %s (", mc.op)
		}
	}

	io.WriteString(w, ")")

	return nil
}
