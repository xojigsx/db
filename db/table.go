package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type hasbase interface{ base() *Base }

type Base struct {
	ID        string     `json:"id" db:"id"`
	CreatedAt time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt time.Time  `json:"updated_at" db:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
}

func (b *Base) base() *Base { return b }

type Table[B any] struct {
	prefix string
	stmt   cache[sqlx.Stmt]
	nstmt  cache[sqlx.NamedStmt]
	db     *sqlx.DB
	b      *builder
}

func NewTable[B any, T interface {
	*B
	hasbase
}](name, prefix string, db *sqlx.DB) (*Table[B], error) {
	b := newBuilder[B](db.Mapper, name)

	t := &Table[B]{
		prefix: prefix,
		db:     db,
		stmt: cache[sqlx.Stmt]{
			ctor:  (*sqlx.DB).Preparex,
			build: b.build,
		},
		nstmt: cache[sqlx.NamedStmt]{
			ctor:  (*sqlx.DB).PrepareNamed,
			build: b.build,
		},
		b: b,
	}

	return t, nil
}

func (t *Table[B]) prepare(name string, args ...any) (*query, *sqlx.Stmt, error) {
	q, err := t.b.query(t.db.DriverName(), name, args...)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := t.stmt.prepare(t.db, q)
	if err != nil {
		return nil, nil, err
	}

	return q, stmt, nil
}

func (t *Table[B]) prepareNamed(name string, args ...any) (*query, *sqlx.NamedStmt, error) {
	q, err := t.b.query(t.db.DriverName(), name, args...)
	if err != nil {
		return nil, nil, err
	}

	stmt, err := t.nstmt.prepare(t.db, q)
	if err != nil {
		return nil, nil, err
	}

	return q, stmt, nil
}

func (t *Table[B]) Insert(ctx context.Context, b *B) error {
	base := interface{}(b).(hasbase).base()
	base.ID = ID(t.prefix)

	_, stmt, err := t.prepareNamed("insert")
	if err != nil {
		return fmt.Errorf("error preparing insert statement: %w", err)
	}

	rows, err := stmt.QueryContext(ctx, b)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		if err = rows.Scan(&base.CreatedAt, &base.UpdatedAt); err != nil {
			return err
		}
	}

	return nil
}

func (t *Table[B]) Delete(ctx context.Context, id string) error {
	_, stmt, err := t.prepare("delete")
	if err != nil {
		return fmt.Errorf("error preparing delete statement: %w", err)
	}

	res, err := stmt.ExecContext(ctx, id)
	if err != nil {
		return err
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (t *Table[B]) Select(ctx context.Context, args ...any) ([]*B, error) {
	q, stmt, err := t.prepare("select", args...)
	if err != nil {
		return nil, fmt.Errorf("error preparing select statement: %w", err)
	}

	var bs []*B

	switch err := stmt.SelectContext(ctx, &bs, q.values...); {
	case errors.Is(err, sql.ErrNoRows) || len(bs) == 0:
		return nil, sql.ErrNoRows
	case err != nil:
		return nil, fmt.Errorf("error executing select statement: %w", err)
	}

	return bs, nil
}

func (t *Table[B]) Update(ctx context.Context, id string, args ...any) error {
	q, stmt, err := t.prepare("update", append(args, "id", id)...)
	if err != nil {
		return fmt.Errorf("error preparing select statement: %w", err)
	}

	res, err := stmt.ExecContext(ctx, q.values...)
	if err != nil {
		return fmt.Errorf("error executing update statement: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (t *Table[B]) Close() error {
	return errors.Join(
		t.stmt.Close(),
		t.nstmt.Close(),
	)
}
