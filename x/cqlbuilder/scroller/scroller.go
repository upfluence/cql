package scroller

import (
	"context"
	"errors"
	"maps"
	"math"
	"strings"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/x/cqlbuilder"
)

// Statement scrolls a table by token. PrimaryKeys are the partition key
// columns. A page starts after the last token of the previous one: without
// Distinct, the remaining rows of a partition overlapping two pages are skipped.
type Statement struct {
	Table string

	PrimaryKeys []string
	Fields      []cqlbuilder.Marker
	Consistency cql.Consistency

	// Distinct scrolls one row per partition, so a partition never overlaps
	// two pages. Fields must then only contain static columns.
	Distinct bool

	// WhereClause is added to the token restriction and Values holds the
	// values of its markers, the "token" binding is reserved. Restricting
	// columns outside of the whole partition key requires AllowFiltering.
	WhereClause    cqlbuilder.PredicateClause
	Values         map[string]any
	AllowFiltering bool

	ScrollSize       int
	ErrorHandler     func(error) error
	EndOfPageHandler func()
}

func (s *Statement) tokenMarker() cqlbuilder.Marker {
	return cqlbuilder.CQLExpression("token", "TOKEN("+strings.Join(s.PrimaryKeys, ", ")+")")
}

func (s *Statement) selectClauses() []cqlbuilder.Marker {
	var res = []cqlbuilder.Marker{s.tokenMarker()}

	res = append(res, s.Fields...)

	for _, pk := range s.PrimaryKeys {
		res = append(res, cqlbuilder.Column(pk))
	}

	return res
}

func (s *Statement) whereClause() cqlbuilder.PredicateClause {
	var pc = cqlbuilder.Gt(s.tokenMarker())

	if s.WhereClause == nil {
		return pc
	}

	return cqlbuilder.And(pc, s.WhereClause)
}

func (s *Statement) values(token int64) map[string]any {
	var vs = make(map[string]any, len(s.Values)+1)

	maps.Copy(vs, s.Values)
	vs["token"] = token

	return vs
}

func (s *Statement) handleError(err error) error {
	if s.ErrorHandler != nil {
		return s.ErrorHandler(err)
	}

	return err
}

func (s *Statement) limit() int {
	if s.ScrollSize > 0 {
		return s.ScrollSize
	}

	return 2048
}

type shadowScanner struct {
	cur cqlbuilder.Cursor

	ok    bool
	token int64
}

var ErrSkip = errors.New("skip")

func (sc *shadowScanner) Scan(vs map[string]any) error {
	vs["token"] = &sc.token

	sc.ok = sc.cur.Scan(vs)

	if !sc.ok {
		return ErrSkip
	}

	return nil
}

type Scroller struct {
	QueryBuilder cqlbuilder.QueryBuilder
}

func (s *Scroller) Scroll(ctx context.Context, stmt Statement, scanner func(cqlbuilder.Scanner) error) error {
	var (
		nextToken = int64(math.MinInt64)
		q         = s.QueryBuilder.PrepareSelect(
			cqlbuilder.SelectStatement{
				Table:          stmt.Table,
				Distinct:       stmt.Distinct,
				SelectClauses:  stmt.selectClauses(),
				WhereClause:    stmt.whereClause(),
				Limit:          cqlbuilder.NullableInt{Valid: true, Int: stmt.limit()},
				Consistency:    stmt.Consistency,
				AllowFiltering: stmt.AllowFiltering,
			},
		)
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		cur := q.Query(ctx, stmt.values(nextToken))
		ok := true
		hasResult := false
		pageMaxToken := nextToken

		for ok {
			var sc = shadowScanner{cur: cur}

			if err := scanner(&sc); err != nil && !errors.Is(err, ErrSkip) {
				if terr := stmt.handleError(err); terr != nil {
					return terr
				}
			}

			ok = sc.ok

			if ok {
				hasResult = true

				if sc.token > pageMaxToken {
					pageMaxToken = sc.token
				}
			}
		}

		if err := cur.Close(); err != nil {
			if terr := stmt.handleError(err); terr != nil {
				return terr
			}
		}

		if stmt.EndOfPageHandler != nil {
			stmt.EndOfPageHandler()
		}

		if !hasResult || pageMaxToken == nextToken {
			return nil
		}

		nextToken = pageMaxToken
	}
}
