package db_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"jig.sx/usvc/db"
	"jig.sx/usvc/db/dbjson"
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
	db.Base

	Field      string      `json:"field" db:"field"`
	OtherField string      `json:"other_field" db:"other_field"`
	JSONField  dbjson.Type `json:"json_field" db:"json_field"`
}

func TestTable(t *testing.T) {
	var (
		ctx     = context.Background()
		uniqtxt = db.ID("test")
		driver  = os.Getenv("DATABASE_DRIVER")
		dsn     = os.Getenv("DATABASE_URL")
	)

	xdb, err := db.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}

	xdb.MustExec(schema)

	table, err := db.NewTable[Model]("test_table", "tt", xdb)
	if err != nil {
		t.Fatal(err)
	}

	defer table.Close()

	m := &Model{
		Field:      "field",
		OtherField: "other_field",
		JSONField:  dbjson.Object("key", "value"),
	}

	if err := table.Insert(ctx, m); err != nil {
		t.Fatal(err)
	}

	if err := table.Update(ctx, m.ID, "field", "meow value", "json_field", dbjson.Object("unique", dbjson.Object("id", uniqtxt), "foo", dbjson.Array(1, 2))); err != nil {
		t.Fatal(err)
	}

	nm, err := table.Select(ctx, "id", m.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(nm) != 1 {
		t.Fatalf("got %d, want 1", len(nm))
	}

	ms, err := table.Select(ctx, dbjson.TextPath{"json_field", "unique", "id"}, uniqtxt)
	if err != nil {
		t.Fatal(err)
	}

	if len(ms) != 1 {
		t.Fatalf("got %d, want 1", len(ms))
	}

	if ms[0].ID != m.ID {
		t.Fatalf("got %s, want %s", ms[0].ID, m.ID)
	}

	want := dbjson.Object(
		"key", "value",
		"foo", dbjson.Array(1, 2),
		"unique", dbjson.Object("id", uniqtxt),
	)

	got := nm[0].JSONField

	if !dbjson.Equal(got, want) {
		t.Fatalf("got %s, want %s", got, want)
	}

	if err := table.Delete(ctx, m.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := table.Select(ctx, "id", m.ID); err != sql.ErrNoRows {
		t.Fatal("expected sql.ErrNoRows")
	}
}
