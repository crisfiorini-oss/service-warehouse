package duckdb

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// quoteIdent spells s as a DuckDB identifier. Every name that reaches SQL here
// came from a request, so it is always quoted and its quote doubled; checkName
// rejects what quoting cannot make safe.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteLiteral spells s as a DuckDB string literal.
func quoteLiteral(s string) string {
	return `'` + strings.ReplaceAll(s, `'`, `''`) + `'`
}

// checkName rejects a name no quoting can carry to the engine intact: an empty
// one, invalid UTF-8, or a NUL, which the engine's C strings would truncate at.
func checkName(s string) error {
	switch {
	case s == "":
		return errors.New("name is empty")
	case !utf8.ValidString(s):
		return errors.New("name is not valid UTF-8")
	case strings.ContainsRune(s, 0):
		return errors.New("name contains a NUL byte")
	}
	return nil
}

// hasKeyword reports whether the SQL text contains kw as a bare word outside
// string literals, quoted identifiers, comments and dollar-quoted strings. It
// is deliberately conservative the other way too: a word that is really an
// alias still counts, so a statement is refused rather than misread.
func hasKeyword(sql, kw string) bool {
	n := len(sql)
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			i = skipBlockComment(sql, i)
		case c == '\'':
			i = skipQuoted(sql, i, '\'', i > 0 && isEscapeStringPrefix(sql, i-1))
		case c == '"':
			i = skipQuoted(sql, i, '"', false)
		case c == '$':
			if end, ok := dollarQuoteEnd(sql, i); ok {
				i = end
			} else {
				i++
				// $name is a parameter, never the keyword.
				for i < n && isWordByte(sql[i]) {
					i++
				}
			}
		case isWordByte(c):
			start := i
			for i < n && isWordByte(sql[i]) {
				i++
			}
			if strings.EqualFold(sql[start:i], kw) {
				return true
			}
		default:
			i++
		}
	}
	return false
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// isEscapeStringPrefix reports whether the quote that follows index i opens an
// E'...' string, in which a backslash escapes the next byte.
func isEscapeStringPrefix(sql string, i int) bool {
	if sql[i] != 'e' && sql[i] != 'E' {
		return false
	}
	return i == 0 || !isWordByte(sql[i-1])
}

// skipQuoted returns the index after the quoted run that opens at i. A doubled
// quote is an escaped quote; with backslash set a backslash escapes any byte.
func skipQuoted(sql string, i int, quote byte, backslash bool) int {
	n := len(sql)
	i++
	for i < n {
		switch {
		case backslash && sql[i] == '\\':
			i += 2
		case sql[i] == quote && i+1 < n && sql[i+1] == quote:
			i += 2
		case sql[i] == quote:
			return i + 1
		default:
			i++
		}
	}
	return n
}

// skipBlockComment returns the index after the (nested) block comment at i.
func skipBlockComment(sql string, i int) int {
	n, depth := len(sql), 0
	for i < n {
		switch {
		case sql[i] == '/' && i+1 < n && sql[i+1] == '*':
			depth++
			i += 2
		case sql[i] == '*' && i+1 < n && sql[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		default:
			i++
		}
	}
	return n
}

// dollarQuoteEnd returns the index after the dollar-quoted string ($$...$$ or
// $tag$...$tag$) that opens at i, if one does.
func dollarQuoteEnd(sql string, i int) (int, bool) {
	n := len(sql)
	j := i + 1
	for j < n && isWordByte(sql[j]) && !(sql[j] >= '0' && sql[j] <= '9' && j == i+1) {
		j++
	}
	if j >= n || sql[j] != '$' {
		return 0, false
	}
	tag := sql[i : j+1]
	end := strings.Index(sql[j+1:], tag)
	if end < 0 {
		return n, true
	}
	return j + 1 + end + len(tag), true
}
