package clickhouse

import "strings"

// splitSQLStatements splits a migration file into individual statements on
// semicolons, while ignoring semicolons that appear inside string literals,
// backtick-quoted identifiers, line comments (-- ... EOL) and block comments
// (/* ... */). Comments are stripped from the emitted statements.
//
// A naive strings.Split(sql, ";") breaks on a semicolon inside a `--` comment,
// turning one statement into two malformed fragments — the failure that forced
// hand-removing semicolons from migration comments. This splitter removes that
// whole class of bug.
func splitSQLStatements(sql string) []string {
	var statements []string
	var b strings.Builder
	r := []rune(sql)
	n := len(r)

	flush := func() {
		if s := strings.TrimSpace(b.String()); s != "" {
			statements = append(statements, s)
		}
		b.Reset()
	}

	for i := 0; i < n; {
		c := r[i]
		switch {
		case c == '\'': // single-quoted string literal (ClickHouse string)
			b.WriteRune(c)
			i++
			for i < n {
				b.WriteRune(r[i])
				if r[i] == '\'' {
					// '' is an escaped quote inside the literal, not its end.
					if i+1 < n && r[i+1] == '\'' {
						b.WriteRune(r[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
		case c == '`': // backtick-quoted identifier
			b.WriteRune(c)
			i++
			for i < n {
				b.WriteRune(r[i])
				if r[i] == '`' {
					i++
					break
				}
				i++
			}
		case c == '-' && i+1 < n && r[i+1] == '-': // line comment: drop to EOL
			i += 2
			for i < n && r[i] != '\n' {
				i++
			}
			b.WriteRune(' ')
		case c == '/' && i+1 < n && r[i+1] == '*': // block comment: drop to */
			i += 2
			for i+1 < n && !(r[i] == '*' && r[i+1] == '/') {
				i++
			}
			i += 2
			b.WriteRune(' ')
		case c == ';':
			flush()
			i++
		default:
			b.WriteRune(c)
			i++
		}
	}
	flush()
	return statements
}
