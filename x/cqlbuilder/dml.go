package cqlbuilder

import (
	"fmt"
	"io"
	"time"
)

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
		fmt.Fprintf(
			w,
			" TIMESTAMP %d",
			do.Timestamp.Unix()*1000+do.Timestamp.UnixNano()/1000000,
		)
	}
}

type LWTClause interface {
	writeTo(QueryWriter, map[string]interface{}) error
}

type notExistsClause struct{}

var NotExistsClause = notExistsClause{}

func (notExistsClause) writeTo(qw QueryWriter, _ map[string]interface{}) error {
	_, err := io.WriteString(qw, "IF NOT EXISTS")
	return err
}

func (notExistsClause) isInsertClause() {}
func (notExistsClause) isUpdateClause() {}

type existsClause struct{}

var ExistsClause = existsClause{}

func (existsClause) writeTo(qw QueryWriter, _ map[string]interface{}) error {
	_, err := io.WriteString(qw, "IF EXISTS")
	return err
}

func (existsClause) isUpdateClause() {}
func (existsClause) isDeleteClause() {}

type PredicateLWTClause struct {
	Predicate PredicateClause
}

func (plc PredicateLWTClause) writeTo(qw QueryWriter, vs map[string]interface{}) error {
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
