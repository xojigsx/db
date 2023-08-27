package db

import (
	"strings"
	"testing"

	"jig.sx/usvc/db/dbjson"

	"github.com/jmoiron/sqlx/reflectx"
)

var mapper = reflectx.NewMapperFunc("json", strings.ToLower)

type User struct {
	Base

	Name    string      `json:"name"`
	Age     int         `json:"age"`
	Details dbjson.Type `json:"details"`
}

func TestBuilder(t *testing.T) {
	tests := []struct {
		driver string
		tmpl   string
		args   []interface{}
		query  string
	}{
		{
			"postgres", "select",
			[]interface{}{"id", "usr-132", "age", 18},
			"SELECT * FROM users WHERE deleted_at IS NULL AND id = $1 AND age = $2 ORDER BY created_at DESC;",
		},
		{
			"postgres", "insert",
			[]interface{}{},
			"INSERT INTO users (id, name, age, details) VALUES (:id, :name, :age, :details) RETURNING created_at, updated_at;",
		},
		{
			"postgres", "update",
			[]interface{}{"age", 18, "id", "usr-132"},
			"UPDATE users SET updated_at = NOW(), age = $1 WHERE deleted_at IS NULL AND id = $2 RETURNING updated_at;",
		},
		{
			"postgres", "update",
			[]interface{}{"age", 18, "name", "John", dbjson.Path{"details", "key"}, dbjson.Object("foo", "bar"), "id", "usr-132"},
			"UPDATE users SET updated_at = NOW(), age = $1, name = $2, details->'key' = (CASE WHEN details->'key' = 'null'::jsonb THEN '{}'::jsonb ELSE details->'key' END) || (CASE WHEN $3 = 'null'::jsonb THEN '{}'::jsonb ELSE $3 END) WHERE deleted_at IS NULL AND id = $4 RETURNING updated_at;",
		},
		{
			"mysql", "delete",
			[]interface{}{"id", "usr-132", "age", 18},
			"UPDATE users SET deleted_at = NOW() WHERE deleted_at IS NULL AND id = $1;",
		},
		{
			"mysql", "select",
			[]interface{}{"id", "usr-132", "age", 18},
			"SELECT * FROM users WHERE deleted_at IS NULL AND id = $1 AND age = $2 ORDER BY created_at DESC;",
		},
		{
			"mysql", "insert",
			[]interface{}{},
			"INSERT INTO users (id, name, age, details) VALUES (:id, :name, :age, :details);",
		},
		{
			"mysql", "update",
			[]interface{}{"age", 18, "id", "usr-132"},
			"UPDATE users SET updated_at = NOW(), age = $1 WHERE deleted_at IS NULL AND id = $2;",
		},
		{
			"mysql", "update",
			[]interface{}{"age", 18, "name", "John", dbjson.Path{"details", "key"}, dbjson.Object("foo", "bar"), "id", "usr-132"},
			"UPDATE users SET updated_at = NOW(), age = $1, name = $2, details->'key' = JSON_MERGE_PATCH(details->'key', $3) WHERE deleted_at IS NULL AND id = $4;",
		},
		{
			"mysql", "delete",
			[]interface{}{"id", "usr-132", "age", 18},
			"UPDATE users SET deleted_at = NOW() WHERE deleted_at IS NULL AND id = $1;",
		},
	}

	b := newBuilder[User](mapper, "users")

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			q, err := b.query(test.driver, test.tmpl, test.args...)
			if err != nil {
				t.Fatal(err)
			}

			raw, err := b.build(q)
			if err != nil {
				t.Fatal(err)
			}

			if raw != test.query {
				t.Fatalf("%s: expected %q, got %q", test.driver, test.query, raw)
			}
		})
	}
}
