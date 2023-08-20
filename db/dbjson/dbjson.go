package dbjson

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx/types"
)

type Type = types.JSONText

func Object(keyval ...any) Type {
	if len(keyval)%2 != 0 {
		panic("Object: invalid keyval pair")
	}

	m := make(map[string]any, len(keyval)/2)

	for i := 0; i < len(keyval); i += 2 {
		m[keyval[i].(string)] = keyval[i+1]
	}

	p, err := json.Marshal(m)
	if err != nil {
		panic("unexpected error: " + err.Error())
	}

	return Type(p)
}

type Ref interface {
	Column() string
	Expr() string
}

type TextPath []string

var _ Ref = TextPath(nil)

func (tp TextPath) Column() string { return Path(tp).Column() }

func (tp TextPath) Expr() string { return Path(tp).expr("->", "->>") }

type Path []string

var _ Ref = Path(nil)

var sq = strings.NewReplacer(
	`'`, ``,
	`"`, ``,
)

func (p Path) Column() string {
	if len(p) == 0 {
		panic("unexpected empty path")
	}
	return p[0]
}

func (p Path) Expr() string { return p.expr("->", "->") }

func (p Path) expr(sep, last string) string {
	switch len(p) {
	case 0, 1:
		return p.Column()
	case 2:
		return p.Column() + last + "'" + sq.Replace(p[1]) + "'"
	default:
		q := p.Column()

		for _, s := range p[1 : len(p)-1] {
			q += sep + "'" + sq.Replace(s) + "'"
		}

		return q + last + "'" + sq.Replace(p[len(p)-1]) + "'"
	}
}

func Array(items ...any) Type {
	return Text(items)
}

func Text(v any) Type {
	p, err := json.Marshal(v)
	if err != nil {
		panic("unexpected error: " + err.Error())
	}

	return Type(p)
}

func Equal(lhs, rhs Type) bool {
	var l, r any

	if err := json.Unmarshal(lhs, &l); err != nil {
		panic("unexpected error: " + err.Error())
	}

	if err := json.Unmarshal(rhs, &r); err != nil {
		panic("unexpected error: " + err.Error())
	}

	return reflect.DeepEqual(l, r)
}
