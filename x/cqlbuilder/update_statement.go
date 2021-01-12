package cqlbuilder

import (
	"errors"
	"fmt"
)

var (
	errMissingUpdateValue = errors.New("x/cqlbuilder: missing value of the key for update")
	errNoUpdates          = errors.New("x/cqlbuilder: no update given")
)

type LWTUpdateClause interface {
	LWTClause

	isUpdateClause()
}

type UpdateOperation interface {
	WriteTo(QueryWriter, string, interface{}, bool) error
	Clone() UpdateOperation
}

type set struct{}

func (set) WriteTo(qw QueryWriter, k string, v interface{}, ok bool) error {
	if !ok {
		return errMissingUpdateValue
	}

	fmt.Fprintf(qw, "%s = ?", k)
	qw.AddArg(v)

	return nil
}

func (set) Clone() UpdateOperation { return set{} }

var Set = set{}

type UpdateClause struct {
	Field Marker
	Op    UpdateOperation
}

func (uc UpdateClause) writeTo(qw QueryWriter, qvs map[string]interface{}) error {
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

type UpdateStatement struct {
	Table string

	UpdateClauses []UpdateClause
	WhereClause   PredicateClause

	Options   DMLOptions
	LWTClause LWTUpdateClause
}

func (us UpdateStatement) casScanKeys() []string {
	if lck, ok := us.LWTClause.(interface{ keys() []string }); ok {
		return lck.keys()
	}

	return nil
}

func (us UpdateStatement) buildQuery(qvs map[string]interface{}) (string, []interface{}, error) {
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

	return qw.String(), qw.args, nil
}
