package dbjson_test

import (
	"testing"

	"jig.sx/usvc/db/dbjson"
)

func TestPath(t *testing.T) {
	tests := map[string]dbjson.TextPath{
		`foo`:                       {"foo"},
		`foo->>'bar'`:               {"foo", "bar"},
		`foo->'bar'->>'baz'`:        {"foo", "bar", "baz"},
		`foo->'bar'->'baz'->>'qux'`: {"foo", "bar", "baz", "qux"},
	}

	for want, path := range tests {
		t.Run("", func(t *testing.T) {
			got := path.Expr()

			if got != want {
				t.Errorf("got %q, want %q", got, want)
			}
		})
	}
}
