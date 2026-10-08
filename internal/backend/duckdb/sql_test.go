package duckdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasKeyword(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want bool
	}{
		{"bare", "INSERT INTO t VALUES (1) RETURNING id", true},
		{"lower case", "insert into t values (1) returning *", true},
		{"mixed case", "DELETE FROM t Returning id", true},
		{"after a newline", "INSERT INTO t VALUES (1)\nRETURNING id", true},
		{"none", "INSERT INTO t VALUES (1)", false},
		{"in a string", "INSERT INTO t VALUES ('returning')", false},
		{"in an escaped string", "INSERT INTO t VALUES ('it''s returning')", false},
		{"after an escaped quote", "INSERT INTO t VALUES ('it''s') RETURNING id", true},
		{"in a quoted identifier", `INSERT INTO "returning" VALUES (1)`, false},
		{"in an escaped identifier", `INSERT INTO "a""returning" VALUES (1)`, false},
		{"in a line comment", "INSERT INTO t VALUES (1) -- returning\n", false},
		{"after a line comment", "INSERT INTO t VALUES (1) -- note\nRETURNING id", true},
		{"in a block comment", "INSERT INTO t /* returning */ VALUES (1)", false},
		{"in a nested block comment", "INSERT INTO t /* a /* returning */ b */ VALUES (1)", false},
		{"after a block comment", "INSERT INTO t VALUES (1) /* x */ RETURNING id", true},
		{"in a dollar-quoted string", "INSERT INTO t VALUES ($$returning$$)", false},
		{"in a tagged dollar-quoted string", "INSERT INTO t VALUES ($q$ returning $q$)", false},
		{"after a dollar-quoted string", "INSERT INTO t VALUES ($$x$$) RETURNING id", true},
		{"a parameter of that name", "INSERT INTO t VALUES ($returning)", false},
		{"a numbered parameter then the keyword", "INSERT INTO t VALUES ($1) RETURNING id", true},
		{"inside a longer word", "INSERT INTO returning_log VALUES (1)", false},
		{"as a suffix", "INSERT INTO log_returning VALUES (1)", false},
		{"in an escape string", `INSERT INTO t VALUES (E'it\'s returning')`, false},
		{"after an escape string", `INSERT INTO t VALUES (E'it\'s') RETURNING id`, true},
		{"unterminated string", "INSERT INTO t VALUES ('returning", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hasKeyword(tc.sql, "returning"), tc.sql)
		})
	}
}

func TestQuoting(t *testing.T) {
	require.Equal(t, `"a""b"`, quoteIdent(`a"b`))
	require.Equal(t, `'it''s'`, quoteLiteral("it's"))
	require.NoError(t, checkName("events"))
	require.NoError(t, checkName(`odd " name`))
	require.Error(t, checkName(""))
	require.Error(t, checkName("a\x00b"))
	require.Error(t, checkName("\xff"))
}
