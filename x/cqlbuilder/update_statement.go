package cqlbuilder

import (
	"fmt"

	"github.com/upfluence/cql"
	"github.com/upfluence/errors"
)

var (
	errMissingUpdateValue = errors.New("missing value of the key for update")
	errNoUpdates          = errors.New("no update given")
)

// LWTUpdateClause represents a lightweight transaction (IF) clause for UPDATE statements.
type LWTUpdateClause interface {
	LWTClause

	isUpdateClause()
}

// UpdateOperation defines how a field should be updated (e.g., set, add, remove).
type UpdateOperation interface {
	WriteTo(QueryWriter, string, any, bool) error
	Clone() UpdateOperation
}

type set struct{}

func (set) WriteTo(qw QueryWriter, k string, v any, ok bool) error {
	if !ok {
		return errMissingUpdateValue
	}

	fmt.Fprintf(qw, "%s = ?", k)
	qw.AddArg(v)

	return nil
}

func (set) Clone() UpdateOperation { return set{} }

var Set = set{}

type setOp struct{ op string }

func (sp setOp) WriteTo(qw QueryWriter, k string, v any, ok bool) error {
	if !ok {
		return errMissingUpdateValue
	}

	fmt.Fprintf(qw, "%s = %s %s ?", k, k, sp.op)
	qw.AddArg(v)

	return nil
}

func (sp setOp) Clone() UpdateOperation { return sp }

var (
	SetAdd    = setOp{op: "+"}
	SetRemove = setOp{op: "-"}
)

// UpdateClause specifies a field and operation for an UPDATE statement's SET clause.
type UpdateClause struct {
	Field Marker
	Op    UpdateOperation
}

func (uc UpdateClause) writeTo(qw QueryWriter, qvs map[string]any) error {
	var (
		k     = uc.Field.Binding()
		v, ok = qvs[k]
	)

	err := uc.Op.WriteTo(qw, columnName(uc.Field), v, ok)

	if err == errMissingUpdateValue {
		return ErrMissingKey{Key: k}
	}

	return err
}

// UpdateStatement represents a CQL UPDATE query with SET, WHERE, and optional IF clauses.
type UpdateStatement struct {
	Table string

	UpdateClauses []UpdateClause
	WhereClause   PredicateClause

	Options     DMLOptions
	LWTClause   LWTUpdateClause
	Consistency cql.Consistency
}

func (us UpdateStatement) casScanKeys() []string {
	if lck, ok := us.LWTClause.(interface{ keys() []string }); ok {
		return lck.keys()
	}

	return nil
}

func (us UpdateStatement) buildQuery(qvs map[string]any) (string, []any, error) {
	var qw queryWriter

	if len(us.UpdateClauses) == 0 {
		return "", nil, errNoUpdates
	}

	fmt.Fprintf(&qw, "UPDATE %s", us.Table)
	us.Options.writeTo(&qw)
	qw.WriteString(" SET ")

	for i, uc := range us.UpdateClauses {
		if err := uc.writeTo(&qw, qvs); err != nil {
			return "", nil, err
		}

		if i < len(us.UpdateClauses)-1 {
			qw.WriteString(", ")
		}
	}

	qw.WriteString(" WHERE ")

	if err := us.WhereClause.WriteTo(&qw, qvs); err != nil {
		return "", nil, err
	}

	if lc := us.LWTClause; lc != nil {
		qw.WriteRune(' ')

		if err := lc.writeTo(&qw, qvs); err != nil {
			return "", nil, err
		}
	}

	if us.Consistency > cql.Any {
		qw.args = append(qw.args, cql.WithConsistency(us.Consistency))
	}

	return qw.String(), qw.args, nil
}
