package scroller

import (
	"context"
	"errors"
	"maps"
	"strings"

	"github.com/upfluence/cql"
	"github.com/upfluence/cql/x/cqlbuilder"
)

// NoLimit scrolls without a page limit, see Statement.ScrollSize.
const NoLimit = -1

// Statement scrolls a table by token. PrimaryKeys are the partition key
// columns. A page starts after the last token of the previous one: unless
// Distinct or NoLimit is used, the remaining rows of a partition overlapping two
// pages are skipped.
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

	// Range restricts the scroll to a range of the token ring, the whole ring
	// is scrolled when nil. The "token_to" binding is reserved.
	Range *TokenRange

	// ScrollSize is the number of rows per page, 2048 when zero. With NoLimit,
	// a page is a single query paged by the driver: the rows of a partition
	// are then never skipped.
	ScrollSize       int
	ErrorHandler     func(error) error
	EndOfPageHandler func()
}

func (s *Statement) tokenMarker() cqlbuilder.Marker {
	return cqlbuilder.CQLExpression("token", s.tokenExpression())
}

func (s *Statement) upperTokenMarker() cqlbuilder.Marker {
	return cqlbuilder.CQLExpression("token_to", s.tokenExpression())
}

func (s *Statement) tokenExpression() string {
	return "TOKEN(" + strings.Join(s.PrimaryKeys, ", ") + ")"
}

func (s *Statement) tokenRange() TokenRange {
	if s.Range == nil {
		return FullTokenRange
	}

	return *s.Range
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
	var pcs = []cqlbuilder.PredicateClause{cqlbuilder.Gt(s.tokenMarker())}

	if s.Range != nil {
		pcs = append(pcs, cqlbuilder.Lte(s.upperTokenMarker()))
	}

	if s.WhereClause != nil {
		pcs = append(pcs, s.WhereClause)
	}

	if len(pcs) == 1 {
		return pcs[0]
	}

	return cqlbuilder.And(pcs...)
}

func (s *Statement) values(token int64) map[string]any {
	var vs = make(map[string]any, len(s.Values)+2)

	maps.Copy(vs, s.Values)
	vs["token"] = token

	if s.Range != nil {
		vs["token_to"] = s.Range.To
	}

	return vs
}

func (s *Statement) handleError(err error) error {
	if s.ErrorHandler != nil {
		return s.ErrorHandler(err)
	}

	return err
}

func (s *Statement) limit() cqlbuilder.NullableInt {
	switch {
	case s.ScrollSize < 0:
		return cqlbuilder.NullableInt{}
	case s.ScrollSize > 0:
		return cqlbuilder.NullableInt{Valid: true, Int: s.ScrollSize}
	default:
		return cqlbuilder.NullableInt{Valid: true, Int: 2048}
	}
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
		tokenRange = stmt.tokenRange()
		nextToken  = tokenRange.From
		q          = s.QueryBuilder.PrepareSelect(
			cqlbuilder.SelectStatement{
				Table:          stmt.Table,
				Distinct:       stmt.Distinct,
				SelectClauses:  stmt.selectClauses(),
				WhereClause:    stmt.whereClause(),
				Limit:          stmt.limit(),
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

		if !hasResult || pageMaxToken == nextToken || pageMaxToken >= tokenRange.To {
			return nil
		}

		nextToken = pageMaxToken
	}
}
