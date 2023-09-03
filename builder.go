package qp

import (
	"crypto/sha256"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"text/template"

	"jig.sx/qp/qpjson"

	"github.com/Masterminds/sprig/v3"
	"github.com/jmoiron/sqlx/reflectx"
)

var defaultDialects = make(dialects).
	add("mysql", mysqlQueries).
	add("postgres", postgresqlQueries)

var funcs = template.FuncMap{
	"pjoin": func(prefix, sep string, s []string) string {
		sCopy := make([]string, len(s))
		copy(sCopy, s)
		for i, v := range s {
			sCopy[i] = prefix + v
		}
		return strings.Join(sCopy, sep)
	},
	"isjson": func(v any) bool {
		_, ok := v.(qpjson.Type)
		return ok
	},
}

type dialects map[string]*template.Template

func (d dialects) add(driver string, rawtemplate string) dialects {
	base, ok := d[driver]
	if !ok {
		base = template.New(driver).Funcs(sprig.FuncMap()).Funcs(funcs)
		d[driver] = base
	}

	if _, err := base.Parse(rawtemplate); err != nil {
		panic("unexpected error: " + err.Error())
	}

	return d
}

var baseFields = map[string]struct{}{
	"id":         {},
	"created_at": {},
	"updated_at": {},
	"deleted_at": {},
}

type builder struct {
	dialects dialects
	fields   []string
	table    string
	prefix   string
}

func newBuilder[T any](m *reflectx.Mapper, table string) *builder {
	var (
		t T
		b = &builder{
			table: table,
		}
	)

	for _, fi := range m.TypeMap(reflect.TypeOf(t)).Index {
		if _, ok := baseFields[fi.Name]; ok || fi.Embedded || strings.Contains(fi.Name, ".") {
			continue
		}

		b.fields = append(b.fields, fi.Name)
	}

	return b
}

type ctx struct {
	Table   string
	Fields  []string
	Columns []string
	Values  []any
}

type query struct {
	key     [sha256.Size]byte
	driver  string
	name    string
	columns []string
	values  []any
}

func (b *builder) query(driver, name string, args ...any) (*query, error) {
	columns, values, err := b.split(args...)
	if err != nil {
		return nil, err
	}

	h := sha256.New()

	io.WriteString(h, driver)
	io.WriteString(h, name)

	for _, col := range columns {
		io.WriteString(h, col)
	}

	p := h.Sum(nil)
	q := (*[sha256.Size]byte)(p[:])

	return &query{
		driver:  driver,
		name:    name,
		key:     *q,
		columns: columns,
		values:  values,
	}, nil
}

func (b *builder) build(q *query) (string, error) {
	t := b.template(q.driver)
	if t == nil {
		return "", fmt.Errorf("no template for %q driver", q.driver)
	}

	var buf strings.Builder

	if err := t.ExecuteTemplate(&buf, q.name, ctx{
		Table:   b.table,
		Fields:  b.fields,
		Columns: q.columns,
		Values:  q.values,
	}); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func (b *builder) template(driver string) *template.Template {
	if b.dialects != nil {
		return b.dialects[driver]
	}

	return defaultDialects[driver]
}

func (b *builder) split(args ...any) (columns []string, values []any, err error) {
	if len(args)%2 != 0 {
		return nil, nil, fmt.Errorf("args length is not even")
	}

	for i := 0; i < len(args); i += 2 {
		var column, expr string

		switch v := args[i].(type) {
		case qpjson.Ref:
			column, expr = v.Column(), v.Expr()
		case string:
			column, expr = v, v
		default:
			return nil, nil, fmt.Errorf("invalid %[1]d field %[2]q (%[2]T)", i, args[i])
		}

		if _, ok := baseFields[column]; !ok && !slices.Contains(b.fields, column) {
			return nil, nil, fmt.Errorf("invalid %d field %q", i, column)
		}

		columns = append(columns, expr)
		values = append(values, args[i+1])
	}

	return columns, values, nil
}

const mysqlQueries = `
{{- define "select" -}}
SELECT * FROM {{ .Table }} WHERE deleted_at IS NULL{{ range $i, $col := .Columns }} AND {{ $col }} = ${{ (add $i 1) }}{{ end }} ORDER BY created_at DESC;
{{- end }}

{{- define "insert" -}}
INSERT INTO {{ .Table }} (id, {{ join ", " .Fields }}) VALUES (:id, {{ pjoin ":" ", " .Fields }});
{{- end }}

{{- define "update" -}}
{{- $v := .Values -}}
UPDATE {{ .Table }} SET updated_at = NOW(){{ range $i, $col := (slice .Columns 0 (sub (len .Columns) 1) ) -}}
{{- if isjson (index $v $i) -}}
, {{ $col }} = JSON_MERGE_PATCH({{ $col }}, ${{ add $i 1 }})
{{- else -}}
, {{ $col }} = ${{ add $i 1 -}}
{{- end -}}
{{- end }} WHERE deleted_at IS NULL AND id = ${{ len .Values }};
{{- end }}

{{- define "delete" -}}
UPDATE {{ .Table }} SET deleted_at = NOW() WHERE deleted_at IS NULL AND id = $1;
{{- end }}
`

const postgresqlQueries = `
{{- define "select" -}}
SELECT * FROM {{ .Table }} WHERE deleted_at IS NULL{{ range $i, $col := .Columns }} AND {{ $col }} = ${{ (add $i 1) }}{{ end }} ORDER BY created_at DESC;
{{- end }}

{{- define "insert" -}}
INSERT INTO {{ .Table }} (id, {{ join ", " .Fields }}) VALUES (:id, {{ pjoin ":" ", " .Fields }}) RETURNING created_at, updated_at;
{{- end }}

{{- define "update" -}}
{{- $v := .Values -}}
UPDATE {{ .Table }} SET updated_at = NOW(){{ range $i, $col := (slice .Columns 0 (sub (len .Columns) 1) ) -}}
{{- if isjson (index $v $i) -}}
, {{ $col }} = (CASE WHEN {{ $col }} = 'null'::jsonb THEN '{}'::jsonb ELSE {{ $col }} END) || (CASE WHEN ${{ add $i 1 }} = 'null'::jsonb THEN '{}'::jsonb ELSE ${{ add $i 1 }} END)
{{- else -}}
, {{ $col }} = ${{ add $i 1 -}}
{{- end -}}
{{- end }} WHERE deleted_at IS NULL AND id = ${{ len .Values }} RETURNING updated_at;
{{- end }}

{{- define "delete" -}}
UPDATE {{ .Table }} SET deleted_at = NOW() WHERE deleted_at IS NULL AND id = $1 RETURNING deleted_at;
{{- end }}
`
