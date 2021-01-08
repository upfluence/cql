package migration

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"sort"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/upfluence/log"
)

var ErrNotExist = errors.New("x/migration: This migration does not exist")

type Migration interface {
	ID() uint

	Up() (io.ReadCloser, error)
	Down() (io.ReadCloser, error)
}

type Source interface {
	Get(context.Context, uint) (Migration, error)

	First(context.Context) (Migration, error)
	Next(context.Context, uint) (bool, uint, error)
	Prev(context.Context, uint) (bool, uint, error)
}

type StaticFetcher func(string) ([]byte, error)

type migration struct {
	id   uint
	name string

	fetcher func(string) (io.ReadCloser, error)

	up   string
	down string
}

func (m *migration) ID() uint { return m.id }

func (m *migration) Up() (io.ReadCloser, error) {
	if m.up == "" {
		return nil, ErrNotExist
	}

	return m.fetcher(m.up)
}

func (m *migration) Down() (io.ReadCloser, error) {
	if m.down == "" {
		return nil, ErrNotExist
	}

	return m.fetcher(m.down)
}

type migrations []*migration

func (m migrations) Len() int               { return len(m) }
func (m migrations) Less(i int, j int) bool { return m[i].id < m[j].id }
func (m migrations) Swap(i int, j int)      { m[i], m[j] = m[j], m[i] }

type staticSource struct {
	ms migrations
}

func (s *staticSource) findPos(id uint) (int, error) {
	for i, m := range s.ms {
		if m.id == id {
			return i, nil
		}
	}

	return 0, ErrNotExist
}

func (s *staticSource) Get(_ context.Context, id uint) (Migration, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return nil, err
	}

	return s.ms[i], nil
}

func (s *staticSource) First(context.Context) (Migration, error) {
	if len(s.ms) == 0 {
		return nil, ErrNotExist
	}

	return s.ms[0], nil
}

func (s *staticSource) Next(_ context.Context, id uint) (bool, uint, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return false, 0, err
	}

	if len(s.ms) == i+1 {
		return false, 0, nil
	}

	return true, s.ms[i+1].id, nil
}

func (s *staticSource) Prev(_ context.Context, id uint) (bool, uint, error) {
	var i, err = s.findPos(id)

	if err != nil {
		return false, 0, err
	}

	if len(s.ms) == 0 || i == 0 {
		return false, 0, nil
	}

	return true, s.ms[i-1].id, nil
}

func splitFilename(fname string) (bool, uint, string, bool) {
	var fchunks = strings.Split(fname, ".")

	if len(fchunks) != 3 || (fchunks[1] != "up" && fchunks[1] != "down") ||
		fchunks[2] != "cql" {
		return false, 0, "", false
	}

	mname := fchunks[0]
	mchunks := strings.Split(mname, "_")

	id, err := strconv.Atoi(mchunks[0])

	if id < 0 || err != nil {
		return false, 0, "", false
	}

	return true, uint(id), mname, fchunks[1] == "up"
}

func wrapFetcher(fn StaticFetcher) func(string) (io.ReadCloser, error) {
	return func(fname string) (io.ReadCloser, error) {
		var buf, err = fn(fname)

		if err != nil {
			return nil, err
		}

		return ioutil.NopCloser(bytes.NewReader(buf)), nil
	}
}

func NewStaticSource(fs []string, fn StaticFetcher, logger log.Logger) Source {
	var (
		migrationMap   = make(map[uint]*migration)
		wrappedFetcher = wrapFetcher(fn)
	)

	for _, f := range fs {
		ok, id, name, up := splitFilename(f)

		if !ok {
			logger.Warningf("Can't process %q as a migration file", f)
			continue
		}

		m, ok := migrationMap[id]

		if !ok {
			m = &migration{
				id:      id,
				name:    name,
				fetcher: wrappedFetcher,
			}
		} else if m.name != name {
			logger.Warningf("Name mismatch between %q and %q, for migration %q, skipping it", m.name, name, f)
			continue
		}

		if up {
			m.up = f
		} else {
			m.down = f
		}

		migrationMap[id] = m
	}

	ms := make(migrations, 0, len(migrationMap))

	for _, m := range migrationMap {
		ms = append(ms, m)
	}

	sort.Sort(ms)

	return &staticSource{ms: ms}
}
