package scroller

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/upfluence/cql/x/cqlbuilder"
)

// StatementScroller scrolls the rows of a statement, as Scroller does.
type StatementScroller interface {
	Scroll(context.Context, Statement, func(cqlbuilder.Scanner) error) error
}

// RangeScanner receives the rows of a single token range, always from the same
// goroutine. Close is called once the range is scrolled, even when it failed.
type RangeScanner interface {
	Scan(cqlbuilder.Scanner) error
	Close() error
}

// RangeError reports a token range which could not be scrolled, so it can be
// scrolled again.
type RangeError struct {
	Range TokenRange
	Err   error
}

func (re *RangeError) Error() string {
	return fmt.Sprintf("token range (%d, %d]: %v", re.Range.From, re.Range.To, re.Err)
}

func (re *RangeError) Unwrap() error {
	return re.Err
}

type concurrentOptions struct {
	splits      int
	concurrency int
}

type ConcurrentOption func(*concurrentOptions)

// WithSplits sets the number of token ranges scrolled, 4 times the
// concurrency by default.
func WithSplits(n int) ConcurrentOption {
	return func(o *concurrentOptions) {
		o.splits = n
	}
}

// WithConcurrency sets the number of token ranges scrolled at the same time,
// GOMAXPROCS by default.
func WithConcurrency(n int) ConcurrentOption {
	return func(o *concurrentOptions) {
		o.concurrency = n
	}
}

// ConcurrentScroll splits the token range of stmt, the whole ring when nil, and
// scrolls the ranges concurrently with s, each of them with the RangeScanner
// built by fn. A failing range does not stop the others, every failure is
// returned as a RangeError. fn and the handlers of stmt are called
// concurrently.
func ConcurrentScroll(ctx context.Context, s StatementScroller, stmt Statement, fn func(TokenRange) RangeScanner, opts ...ConcurrentOption) error {
	var o = concurrentOptions{concurrency: runtime.GOMAXPROCS(0)}

	for _, opt := range opts {
		opt(&o)
	}

	o.concurrency = max(o.concurrency, 1)

	if o.splits <= 0 {
		o.splits = 4 * o.concurrency
	}

	var (
		wg sync.WaitGroup

		ranges = stmt.tokenRange().Split(o.splits)
		errs   = make([]error, len(ranges))
		sem    = make(chan struct{}, o.concurrency)
	)

	for i, tr := range ranges {
		wg.Add(1)

		go func() {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			if err := scrollRange(ctx, s, stmt, tr, fn(tr)); err != nil {
				errs[i] = &RangeError{Range: tr, Err: err}
			}
		}()
	}

	wg.Wait()

	return errors.Join(errs...)
}

func scrollRange(ctx context.Context, s StatementScroller, stmt Statement, tr TokenRange, rs RangeScanner) error {
	stmt.Range = &tr

	return errors.Join(s.Scroll(ctx, stmt, rs.Scan), rs.Close())
}
