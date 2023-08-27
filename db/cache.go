package db

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/jmoiron/sqlx"
)

type stmt interface {
	sqlx.Stmt | sqlx.NamedStmt
}

type cache[T stmt] struct {
	ctor  func(*sqlx.DB, string) (*T, error)
	build func(*query) (string, error)
	m     sync.Map
}

func (c *cache[T]) prepare(db *sqlx.DB, q *query) (*T, error) {
	v, ok := c.m.Load(q.key)
	if ok {
		return v.(*T), nil
	}

	raw, err := c.build(q)
	if err != nil {
		return nil, fmt.Errorf("error building query: %w", err)
	}

	slog.Debug("preparing statement",
		"driver", db.DriverName(),
		"type", q.name,
		"sql", raw,
	)

	stmt, err := c.ctor(db, raw)
	if err != nil {
		return nil, fmt.Errorf("error preparing statement: %w", err)
	}

	v, dup := c.m.LoadOrStore(q.key, stmt)
	if dup {
		_ = interface{}(stmt).(io.Closer).Close() // discard the duplicate
		stmt = v.(*T)
	}

	return stmt, nil
}

func (c *cache[T]) Close() error {
	var err error

	c.m.Range(func(_, v interface{}) bool {
		if e := interface{}(v).(io.Closer).Close(); e != nil {
			err = errors.Join(err, e)
		}
		return true
	})

	return err
}
