package cqlbuilder

import (
	"fmt"
	"io"
	"time"
)

// DMLOptions specifies additional options for DML operations such as TTL and timestamp.
type DMLOptions struct {
	TTL       time.Duration
	Timestamp time.Time
}

func (do DMLOptions) writeTo(w io.Writer) {
	if do.TTL == 0 && do.Timestamp.IsZero() {
		return
	}

	io.WriteString(w, " USING")

	if do.TTL > 0 {
		fmt.Fprintf(w, " TTL %d", int(do.TTL.Seconds()))

		if !do.Timestamp.IsZero() {
			io.WriteString(w, " AND")
		}
	}

	if !do.Timestamp.IsZero() {
		fmt.Fprintf(w, " TIMESTAMP %d", do.Timestamp.UnixMicro())
	}
}

// LWTClause represents a lightweight transaction (IF) clause that can be attached to DML statements.
type LWTClause interface {
	writeTo(QueryWriter, map[string]any) error
}

type notExistsClause struct{}

var NotExistsClause = notExistsClause{}

func (notExistsClause) writeTo(qw QueryWriter, _ map[string]any) error {
	_, err := io.WriteString(qw, "IF NOT EXISTS")
	return err
}

func (notExistsClause) isInsertClause() {}
func (notExistsClause) isUpdateClause() {}

type existsClause struct{}

var ExistsClause = existsClause{}

func (existsClause) writeTo(qw QueryWriter, _ map[string]any) error {
	_, err := io.WriteString(qw, "IF EXISTS")
	return err
}

func (existsClause) isUpdateClause() {}
func (existsClause) isDeleteClause() {}

// PredicateLWTClause wraps a PredicateClause as an IF condition for lightweight transactions.
type PredicateLWTClause struct {
	Predicate PredicateClause
}

func (plc PredicateLWTClause) writeTo(qw QueryWriter, vs map[string]any) error {
	if _, err := io.WriteString(qw, "IF "); err != nil {
		return err
	}

	return plc.Predicate.WriteTo(qw, vs)
}

func (plc PredicateLWTClause) keys() []string {
	var (
		ms = plc.Predicate.Markers()
		ks = make([]string, len(ms))
	)

	for i, m := range ms {
		ks[i] = m.Binding()
	}

	return ks
}

func (plc PredicateLWTClause) isUpdateClause() {}
func (plc PredicateLWTClause) isDeleteClause() {}
