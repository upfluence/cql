package cqltest

import (
	"context"
	"io"
	"io/ioutil"
	"strings"

	"github.com/upfluence/cql/x/migration"
)

type StaticSource struct {
	MigrationUp   string
	MigrationDown string
}

func (ss StaticSource) ID() uint {
	return 1
}

func (ss StaticSource) Up() (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.MigrationUp)), nil
}

func (ss StaticSource) Down() (io.ReadCloser, error) {
	return ioutil.NopCloser(strings.NewReader(ss.MigrationDown)), nil
}

func (ss StaticSource) Get(_ context.Context, v uint) (migration.Migration, error) {
	if v != 1 {
		return nil, migration.ErrNotExist
	}

	return ss, nil
}

func (ss StaticSource) First(context.Context) (migration.Migration, error) {
	return ss, nil
}

func (ss StaticSource) Next(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
}

func (ss StaticSource) Prev(context.Context, uint) (bool, uint, error) {
	return false, 0, nil
}
