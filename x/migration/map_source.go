package migration

import (
	"github.com/upfluence/errors"
	"github.com/upfluence/log"
)

var errNotExist = errors.New("entry does not exist")

type mapFetcher map[string]string

func (mf mapFetcher) keys() []string {
	var res = make([]string, 0, len(mf))

	for k := range mf {
		res = append(res, k)
	}

	return res
}

func (mf mapFetcher) fetch(k string) ([]byte, error) {
	v, ok := mf[k]

	if !ok {
		return nil, errNotExist
	}

	return []byte(v), nil
}

func NewMapSource(vs map[string]string, l log.Logger) Source {
	mf := mapFetcher(vs)

	return NewStaticSource(mf.keys(), mf.fetch, l)
}
