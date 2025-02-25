package scroller

import (
	"context"
	"errors"
	"math"
	"strings"

	"github.com/upfluence/cql/x/cqlbuilder"
)

type Statement struct {
	Table string

	PrimaryKeys []string
	Fields      []cqlbuilder.Marker

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
	return cqlbuilder.Gt(s.tokenMarker())
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

func (sc *shadowScanner) Scan(vs map[string]interface{}) error {
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
				Table:         stmt.Table,
				SelectClauses: stmt.selectClauses(),
				WhereClause:   stmt.whereClause(),
				Limit:         cqlbuilder.NullableInt{Valid: true, Int: stmt.limit()},
			},
		)
	)

	for {
		cur := q.Query(ctx, map[string]any{"token": nextToken})
		ok := true
		hasResult := false

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

				if sc.token > nextToken {
					nextToken = sc.token
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

		if !hasResult {
			return nil
		}
	}
}
