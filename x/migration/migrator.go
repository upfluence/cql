package migration

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/upfluence/errors"

	"github.com/upfluence/cql"
)

var (
	ErrConcurrentMigration = errors.New("Concurrent migration running")
	ErrDirty               = errors.New("Migration is dirty")
)

type Migrator interface {
	Up(context.Context) error
	Down(context.Context) error
}

type MultiMigrator []Migrator

func (ms MultiMigrator) Up(ctx context.Context) error {
	var errs []error

	for _, m := range ms {
		if err := m.Up(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.WrapErrors(errs)
}

func (ms MultiMigrator) Down(ctx context.Context) error {
	var errs []error

	for _, m := range ms {
		if err := m.Down(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.WrapErrors(errs)
}

type migrator struct {
	db     cql.DB
	source Source
	opts   options
}

func NewMigrator(db cql.DB, s Source, opts ...Option) Migrator {
	o := defaultOptions

	for _, opt := range opts {
		opt(&o)
	}

	return &migrator{db: db, source: s, opts: o}
}

func (m *migrator) Down(ctx context.Context) error {
	return m.executeUntil(ctx, m.downOne)
}

func (m *migrator) Up(ctx context.Context) error {
	return m.executeUntil(ctx, m.upOne)
}

func (m *migrator) executeUntil(ctx context.Context, fn func(context.Context) (bool, error)) error {
	if err := m.db.Exec(ctx, m.opts.createTableMigrationStmt()); err != nil {
		return errors.Wrap(err, "cant build migration table")
	}

	for {
		if done, err := fn(ctx); done || err != nil {
			return errors.Wrap(err, "migration failed")
		}
	}
}

func (m *migrator) downOne(ctx context.Context) (bool, error) {
	mi, err := m.currentMigration(ctx)

	if mi == nil || err != nil {
		return mi == nil, err
	}

	r, err := mi.Down()

	if err != nil {
		return false, errors.Wrapf(
			err,
			"cant open DOWN migration file for %d",
			mi.ID(),
		)
	}

	defer r.Close()

	if err := m.toggleDirty(ctx, mi.ID(), true); err != nil {
		return false, err
	}

	if err := m.executeMigration(ctx, r); err != nil {
		return false, errors.Wrapf(err, "migration %d", mi.ID())
	}

	return false, executeCAS(
		m.db.ExecCAS(
			ctx,
			m.opts.deleteMigrationStmt(),
			mi.ID(),
		),
		3,
	)
}

func (m *migrator) upOne(ctx context.Context) (bool, error) {
	mi, err := m.nextMigration(ctx)

	if mi == nil || err != nil {
		return mi == nil, err
	}

	r, err := mi.Up()

	if err != nil {
		return false, errors.Wrapf(
			err,
			"cant open UP migration file for %d",
			mi.ID(),
		)
	}

	defer r.Close()

	if err := executeCAS(
		m.db.ExecCAS(
			ctx,
			m.opts.createMigrationStmt(),
			mi.ID(),
			m.opts.clock(),
		),
		3,
	); err != nil {
		return false, err
	}

	if err := m.executeMigration(ctx, r); err != nil {
		return false, errors.Wrapf(err, "migration %d", mi.ID())
	}

	return false, m.toggleDirty(ctx, mi.ID(), false)
}

func executeCAS(cs cql.CASScanner, count int) error {
	var (
		args    = make([]interface{}, count)
		ok, err = cs.ScanCAS(args...)
	)

	if err != nil {
		return err
	}

	if !ok {
		return ErrConcurrentMigration
	}

	return nil
}

func (m *migrator) toggleDirty(ctx context.Context, id uint, dirty bool) error {
	return executeCAS(
		m.db.ExecCAS(
			ctx,
			m.opts.updateMigrationStmt(),
			dirty,
			id,
			!dirty,
		),
		1,
	)
}

func (m *migrator) currentMigration(ctx context.Context) (Migration, error) {
	var num, err = m.currentMigrationID(ctx)

	if err != nil || num == 0 {
		return nil, err
	}

	return m.source.Get(ctx, num)
}

func (m *migrator) nextMigration(ctx context.Context) (Migration, error) {
	var num, err = m.currentMigrationID(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "fetch current migration")
	}

	if num == 0 {
		return m.source.First(ctx)
	}

	ok, next, err := m.source.Next(ctx, num)

	if err != nil || !ok {
		return nil, err
	}

	return m.source.Get(ctx, next)
}

func (m *migrator) currentMigrationID(ctx context.Context) (uint, error) {
	var (
		num, curNum     uint
		dirty, curDirty bool

		cur = m.db.Query(ctx, m.opts.fetchMigrationsStmt())
	)

	for cur.Scan(&curNum, &curDirty) {
		if curNum < num {
			continue
		}

		num = curNum
		dirty = curDirty
	}

	if dirty {
		return 0, ErrDirty
	}

	return num, cur.Close()
}

func (m *migrator) executeMigration(ctx context.Context, r io.Reader) error {
	buf, err := ioutil.ReadAll(r)

	if err != nil {
		return errors.Wrap(err, "cant read migration")
	}

	return errors.Wrap(
		m.db.Exec(ctx, string(buf), cql.WithConsistency(m.opts.consistency)),
		"cant execute migration",
	)
}
