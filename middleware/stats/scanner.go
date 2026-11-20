package stats

import (
	"sync"

	"github.com/upfluence/stats"

	"github.com/upfluence/cql"
)

type scanner struct {
	cql.Scanner

	once      sync.Once
	operation stats.Operation
}

func (s *scanner) Scan(vs ...any) error {
	err := s.Scanner.Scan(vs...)
	s.once.Do(func() { s.operation.Finish(err) })

	return err //nolint:wrapcheck
}

type casScanner struct {
	cql.CASScanner

	once      sync.Once
	operation stats.Operation
}

func (s *casScanner) ScanCAS(vs ...any) (bool, error) {
	applied, err := s.CASScanner.ScanCAS(vs...)
	s.once.Do(func() { s.operation.Finish(err) })

	return applied, err //nolint:wrapcheck
}
