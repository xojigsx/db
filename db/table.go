package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"jig.sx/usvc/db/dbjson"

	"github.com/jmoiron/sqlx"
)

var defaultedFields = map[string]struct{}{
	"created_at": {},
	"updated_at": {},
	"deleted_at": {},
}

var constFields = map[string]struct{}{
	"id":         {},
	"slug":       {},
	"created_at": {},
	"updated_at": {},
	"deleted_at": {},
}

type hasbase interface{ base() *Base }

type Base struct {
	ID        string     `json:"id" db:"id"`
	Slug      string     `json:"slug,omitempty" db:"slug"`
	CreatedAt time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt time.Time  `json:"updated_at" db:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
}

func (b *Base) base() *Base { return b }

type Table[B any] struct {
	db     *sqlx.DB
	name   string
	prefix string
	fields []string

	stmt struct {
		get    func(context.Context, any, ...any) error
		insert func(context.Context, any) (*sql.Rows, error)
		delete func(context.Context, ...any) (sql.Result, error)
		update func(context.Context, string, ...any) (sql.Result, error)
	}

	cleanup []func() error
}

func NewTable[B any, T interface {
	*B
	hasbase
}](name, prefix string, db *sqlx.DB) (*Table[B], error) {
	t := &Table[B]{
		name:   name,
		prefix: prefix,
		db:     db,
	}
	if err := t.init(); err != nil {
		return nil, fmt.Errorf("error initializing table: %w", err)
	}
	return t, nil
}

func (t *Table[B]) init() (err error) {
	var (
		b   B
		buf strings.Builder
	)

	for _, fi := range t.db.Mapper.TypeMap(reflect.TypeOf(b)).Index {
		if fi.Embedded || strings.Contains(fi.Path, ".") {
			continue
		}
		if _, ok := defaultedFields[fi.Name]; ok {
			continue
		}

		t.fields = append(t.fields, fi.Name)
	}

	fmt.Fprintf(&buf, "INSERT INTO %s (%s", t.name, t.fields[0])

	for _, field := range t.fields[1:] {
		fmt.Fprintf(&buf, ", %s", field)
	}

	fmt.Fprintf(&buf, ") VALUES (:%s", t.fields[0])

	for _, field := range t.fields[1:] {
		fmt.Fprintf(&buf, ", :%s", field)
	}

	fmt.Fprintf(&buf, ") RETURNING created_at, updated_at")

	var (
		insert = buf.String()
	)

	buf.Reset()

	switch prep, err := t.db.PrepareNamed(insert); {
	case errors.Is(err, driver.ErrSkip):
		slog.Warn("preparing insert statement not supported by driver", "driver", t.db.DriverName())

		t.stmt.insert = func(ctx context.Context, v any) (*sql.Rows, error) {
			rows, err := t.db.NamedQueryContext(ctx, insert, v)
			if err != nil {
				return nil, fmt.Errorf("error inserting record: %w", err)
			}
			return rows.Rows, nil
		}
	case err != nil:
		return fmt.Errorf("error preparing insert statement: %w", err)
	default:
		t.stmt.insert = prep.QueryContext
		t.cleanup = append(t.cleanup, prep.Close)
	}

	slog.Info("init table",
		"table", t.name,
		"type", "insert",
		"sql", insert,
	)

	var (
		get = fmt.Sprintf("SELECT * FROM %s WHERE deleted_at IS NULL AND (id = $1 OR slug = $1) LIMIT 1", t.name)
	)

	switch prep, err := t.db.Preparex(get); {
	case errors.Is(err, driver.ErrSkip):
		slog.Warn("preparing get statement not supported by driver", "driver", t.db.DriverName())

		t.stmt.get = func(ctx context.Context, v any, args ...any) error {
			return t.db.GetContext(ctx, v, get, args...)
		}
	case err != nil:
		return fmt.Errorf("error preparing get statement: %w", err)
	default:
		t.stmt.get = prep.GetContext
		t.cleanup = append(t.cleanup, prep.Close)
	}

	slog.Info("init table",
		"table", t.name,
		"type", "get",
		"sql", get,
	)

	var (
		delet = fmt.Sprintf("UPDATE %s SET deleted_at = NOW() WHERE deleted_at IS NULL AND (id = $1 OR slug = $1)", t.name)
	)

	switch prep, err := t.db.Preparex(delet); {
	case errors.Is(err, driver.ErrSkip):
		slog.Warn("preparing delete statement not supported by driver", "driver", t.db.DriverName())

		t.stmt.delete = func(ctx context.Context, args ...any) (sql.Result, error) {
			return t.db.ExecContext(ctx, delet, args...)
		}
	case err != nil:
		return fmt.Errorf("error preparing delete statement: %w", err)
	default:
		t.stmt.delete = prep.ExecContext
		t.cleanup = append(t.cleanup, prep.Close)
	}

	// TODO: cache prepared statements per updated column set
	t.stmt.update = func(ctx context.Context, query string, args ...any) (sql.Result, error) {
		return t.db.ExecContext(ctx, query, args...)
	}

	slog.Info("init table",
		"table", t.name,
		"type", "delete",
		"sql", delet,
	)

	return nil
}

func (t *Table[B]) Insert(ctx context.Context, b *B) error {
	var (
		base = interface{}(b).(hasbase).base()
	)

	base.ID = ID(t.prefix)

	if base.Slug == "" {
		base.Slug = base.ID
	}

	rows, err := t.stmt.insert(ctx, b)
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

func (t *Table[B]) Get(ctx context.Context, idslug string) (*B, error) {
	var b B

	if err := t.stmt.get(ctx, &b, idslug); err != nil {
		return nil, err
	}

	return &b, nil
}

func (t *Table[B]) Delete(ctx context.Context, idslug string) error {
	res, err := t.stmt.delete(ctx, idslug)
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

func (t *Table[B]) List(ctx context.Context, columns ...any) ([]*B, error) {
	var (
		query strings.Builder
	)

	fields, values, err := t.split(columns...)
	if err != nil {
		return nil, err
	}

	fmt.Fprintf(&query, "SELECT * FROM %s WHERE deleted_at IS NULL", t.name)

	if len(fields) > 0 {
		fmt.Fprintf(&query, " AND (%s = $1", fields[0])

		for i, field := range fields[1:] {
			fmt.Fprintf(&query, " AND %s = $%d", field, i+2)
		}

		fmt.Fprintf(&query, ")")
	}

	fmt.Fprintf(&query, " ORDER BY created_at DESC")

	var bs []*B

	switch err := t.db.SelectContext(ctx, &bs, query.String(), values...); {
	case errors.Is(err, sql.ErrNoRows) || bs == nil:
		return make([]*B, 0), nil
	case err != nil:
		return nil, err
	}

	return bs, nil
}

func (t *Table[B]) Update(ctx context.Context, idslug string, columns ...any) error {
	var (
		query strings.Builder
	)

	fields, values, err := t.split(columns...)
	if err != nil {
		return err
	}

	if len(fields) == 0 {
		return fmt.Errorf("no fields to update")
	}

	fmt.Fprintf(&query, "UPDATE %s SET updated_at = NOW()", t.name)

	for i, field := range fields {
		if _, ok := values[i].(dbjson.Type); ok {
			fmt.Fprintf(&query, ", %[1]s = (CASE WHEN %[1]s = 'null'::jsonb THEN '{}'::jsonb ELSE %[1]s END) || (CASE WHEN $%[2]d = 'null'::jsonb THEN '{}'::jsonb ELSE $%[2]d END)", field, i+1)
		} else {
			fmt.Fprintf(&query, ", %s = $%d", field, i+1)
		}
	}

	fmt.Fprintf(&query, " WHERE deleted_at IS NULL AND (id = $%[1]d OR slug = $%[1]d)", len(fields)+1)

	res, err := t.stmt.update(ctx, query.String(), append(values, idslug)...)
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

func (t *Table[B]) Close() error {
	var err error
	for _, fn := range t.cleanup {
		err = errors.Join(err, fn())
	}
	return err
}

func (t *Table[B]) split(columns ...any) (fields []string, values []any, err error) {
	if len(columns)%2 != 0 {
		panic("len(columns) is not even")
	}

	for i := 0; i < len(columns); i += 2 {
		var column, expr string

		switch v := columns[i].(type) {
		case dbjson.Ref:
			column, expr = v.Column(), v.Expr()
		case string:
			column, expr = v, v
		default:
			return nil, nil, fmt.Errorf("invalid %[1]d field %[2]q (%[2]T)", i, columns[i])
		}

		if _, ok := constFields[column]; ok {
			return nil, nil, fmt.Errorf("cannot update %d field %q", i, column)
		}

		var ok bool
		for _, field := range t.fields {
			if field == column {
				ok = true
				break
			}
		}

		if !ok {
			return nil, nil, fmt.Errorf("invalid %d field %q", i, column)
		}

		fields = append(fields, expr)
		values = append(values, columns[i+1])
	}

	return fields, values, nil
}
