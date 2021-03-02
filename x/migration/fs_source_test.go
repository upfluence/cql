// +build go1.16

package migration

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/upfluence/log"
)

func TestFSSource(t *testing.T) {
	var (
		ctx = context.Background()
		fs  = fstest.MapFS{
			"3_final.down.cql": &fstest.MapFile{Data: []byte("bar")},
			"2_initial.up.cql": &fstest.MapFile{Data: []byte("foo")},
			"3_final.up.cql":   &fstest.MapFile{Data: []byte("bar")},
		}
	)

	s, err := NewFSSource(fs, log.NewLogger(log.WithSink(sink{})))
	require.NoError(t, err)

	m, err := s.First(ctx)
	require.NoError(t, err)

	assertMigration(t, m, 2, "foo", "")

	_, id, _ := s.Next(ctx, 2)

	assert.Equal(t, uint(3), id)

	m, _ = s.Get(ctx, id)
	assertMigration(t, m, 3, "bar", "bar")

	ok, _, _ := s.Prev(ctx, 2)
	assert.False(t, ok)

	ok, _, err = s.Next(ctx, 3)
	assert.NoError(t, err)
	assert.False(t, ok)
}
