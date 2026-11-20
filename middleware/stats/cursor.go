package stats

import (
	"sync"

	"github.com/upfluence/stats"

	"github.com/upfluence/cql"
)

// cursor finishes its operation on Close: a CQL cursor only reports the
// query error once it is closed.
type cursor struct {
	cql.Cursor

	once        sync.Once
	operation   stats.Operation
	rowsScanned stats.Counter
}

func (c *cursor) Scan(vs ...any) bool {
	ok := c.Cursor.Scan(vs...)

	if ok {
		c.rowsScanned.Inc()
	}

	return ok
}

func (c *cursor) Close() error {
	err := c.Cursor.Close()
	c.once.Do(func() { c.operation.Finish(err) })

	return err //nolint:wrapcheck
}
