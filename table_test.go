package qp_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"jig.sx/qp"
	"jig.sx/qp/qpjson"
)

const schema = `CREATE TABLE IF NOT EXISTS test_table (
  id          VARCHAR(24)   PRIMARY KEY,
  field       VARCHAR(128)  NOT NULL,
  other_field VARCHAR(128)  NOT NULL,
  json_field  JSONB         NOT NULL DEFAULT '{}',
  created_at  TIMESTAMPTZ   DEFAULT now(),
  updated_at  TIMESTAMPTZ   DEFAULT now(),
  deleted_at  TIMESTAMPTZ
);
`

type Model struct {
	qp.Base

	Field      string      `json:"field" db:"field"`
	OtherField string      `json:"other_field" db:"other_field"`
	JSONField  qpjson.Type `json:"json_field" db:"json_field"`
}

func TestTable(t *testing.T) {
	var (
		ctx     = context.Background()
		uniqtxt = qp.ID("test")
		driver  = os.Getenv("DATABASE_DRIVER")
		dsn     = os.Getenv("DATABASE_URL")
	)

	xqp, err := qp.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}

	xqp.MustExec(schema)

	table, err := qp.NewTable[Model]("test_table", "tt", xqp)
	if err != nil {
		t.Fatal(err)
	}

	defer table.Close()

	m := &Model{
		Field:      "field",
		OtherField: "other_field",
		JSONField:  qpjson.Object("key", "value"),
	}

	if err := table.Insert(ctx, m); err != nil {
		t.Fatal(err)
	}

	if err := table.Update(ctx, m.ID, "field", "meow value", "json_field", qpjson.Object("unique", qpjson.Object("id", uniqtxt), "foo", qpjson.Array(1, 2))); err != nil {
		t.Fatal(err)
	}

	nm, err := table.Select(ctx, "id", m.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(nm) != 1 {
		t.Fatalf("got %d, want 1", len(nm))
	}

	ms, err := table.Select(ctx, qpjson.TextPath{"json_field", "unique", "id"}, uniqtxt)
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 1 {
		t.Fatalf("got %d, want 1", len(ms))
	}

	if ms[0].ID != m.ID {
		t.Fatalf("got %s, want %s", ms[0].ID, m.ID)
	}

	want := qpjson.Object(
		"key", "value",
		"foo", qpjson.Array(1, 2),
		"unique", qpjson.Object("id", uniqtxt),
	)

	got := nm[0].JSONField

	if !qpjson.Equal(got, want) {
		t.Fatalf("got %s, want %s", got, want)
	}

	if err := table.Delete(ctx, m.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := table.Select(ctx, "id", m.ID); err != sql.ErrNoRows {
		t.Fatal("expected sql.ErrNoRows")
	}
}
