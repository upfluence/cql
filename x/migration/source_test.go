package migration

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/upfluence/log"
	"github.com/upfluence/log/record"
)

type sink struct{}

func (sink) Log(record.Record) error { return nil }

func assertMigration(t *testing.T, m Migration, id uint, up, down string) {
	if mid := m.ID(); mid != id {
		t.Errorf("migration.ID() = %v, want = %v", mid, id)
	}

	rc, err := m.Up()

	if up == "" && err != ErrNotExist {
		t.Errorf("migration.Up() = %v, want = %v", err, ErrNotExist)
	}

	if rc != nil {
		buf, _ := ioutil.ReadAll(rc)

		if sbuf := string(buf); sbuf != up {
			t.Errorf("migration.Up() = %v, want = %v", sbuf, up)
		}
	}

	rc, err = m.Down()

	if up == "" && err != ErrNotExist {
		t.Errorf("migration.Down() = %v, want = %v", err, ErrNotExist)
	}

	if rc != nil {
		buf, _ := ioutil.ReadAll(rc)

		if sbuf := string(buf); sbuf != down {
			t.Errorf("migration.Down() = %v, want = %v", sbuf, down)
		}
	}
}

func TestFetcher(t *testing.T) {
	s := NewMapSource(
		map[string]string{
			"3_final.down.cql": "bar",
			"2_initial.up.cql": "foo",
			"3_final.up.cql":   "bar",
			"other_file":       "fuz",
		},
		log.NewLogger(log.WithSink(sink{})),
	)

	ctx := context.Background()
	m, err := s.First(ctx)

	if err != nil {
		t.Errorf("source.First() = %v, want = nil", err)
	}

	assertMigration(t, m, 2, "foo", "")

	_, id, _ := s.Next(ctx, 2)

	if id != 3 {
		t.Errorf("source.Next(_, 2) = %v, want = %v", id, 3)
	}

	m, _ = s.Get(ctx, id)
	assertMigration(t, m, 3, "bar", "bar")

	ok, _, _ := s.Prev(ctx, 2)

	if ok {
		t.Errorf("source.Prev(_, 2) = %v, want = %v", ok, false)
	}

	ok, _, err = s.Next(ctx, 3)

	if err != nil {
		t.Errorf("source.Next(_, 3) = %v, want = %v", err, nil)
	}

	if ok {
		t.Errorf("source.Next(_, 3) = %v, want = %v", ok, false)
	}
}
