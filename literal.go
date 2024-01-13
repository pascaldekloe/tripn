package tripn

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

// InNumberWithSign continues from a "+" or "-" in the buffer iff signOffset is 1.
// Otherwise a the start must be a decimal ("0".."9") instead.
func (r *Reader) inNumberWithSign(line []byte, signOffset int, t *Triple) (remainder []byte, err error) {
	i := 1
	for {
		switch {
		case i >= len(line):
			return nil, io.ErrUnexpectedEOF
		case line[i] >= '0' && line[i] <= '9':
			continue
		}
		break // not a decimal
	}

	switch line[i] {
	case ' ', '\t', '\r', '\n': // xsd:integer ended on WS
		if i-signOffset == 0 {
			return nil, r.syntaxErr("sign without number")
		}
		t.DatatypeIRI = XSDInteger
		t.Object = string(line[:i])
		return line[i+1:], nil

	case '.':
		for {
			i++
			if i >= len(line) {
				return nil, io.ErrUnexpectedEOF
			}

			switch line[i] {
			case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
				continue

			default:
				return nil, r.syntaxErr("illegal character in fraction")

			case ' ', '\t', '\r', '\n': // xsd:decimal ended on WS
				if line[i-1] == '.' {
					return nil, r.syntaxErr("decimal with empty fraction")
				}
				t.DatatypeIRI = XSDDecimal
				t.Object = string(line[:i])
				return line[i+1:], nil

			case 'E', 'e':
				break
			}
			break
		}

	case 'E', 'e':
		break

	default:
		return nil, r.syntaxErr("illegal character in number")
	}

	if i-signOffset < 3 { // ".E" or ".e"
		return nil, r.syntaxErr("fraction of double without decimals")
	}

	i++ // pass 'E' or 'e'
	if i < len(line) && (line[i] == '+' || line[i] == '-') {
		i++ // pass sign
	}
	offset := i

	for ; i < len(line); i++ {
		switch line[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			continue
		case ' ', '\t', '\r', '\n': // xsd:decimal ended on WS
			if i == offset {
				return nil, r.syntaxErr("no decimals in double exponent")
			}
			t.DatatypeIRI = XSDDouble
			t.Object = string(line[:i])
			return line[i+1:], nil
		}
		return nil, r.syntaxErr("illegal charater in exponent of double")
	}
	return nil, io.ErrUnexpectedEOF
}

// InDoubleQuote continues from '"' in the buffer.
func (r *Reader) inDoubleQuote(line []byte, t *Triple) (remainder []byte, err error) {
	// long quote (`"""`) option
	if len(line) > 2 && line[1] == '"' && line[2] == '"' {
		for i := 3; i < len(line); i++ {
			switch line[i] {
			case '"': // may terminate
				switch {
				case i+2 >= len(line), line[i+1] != '"':
					continue
				case line[i+2] != '"':
					i++
					continue
				}
				t.Object = string(line[3:i])
				return r.afterQuotedLiteral(line[i+3:], t)
			case '\\': // is escape
				return r.longSingleQuote(line[3:i], line[i:], t)
			}
		}
		return r.longSingleQuote(line[3:], nil, t)
	}

	for i := 1; i < len(line); i++ {
		switch line[i] {
		case '"':
			t.Object = string(line[1:i])
			return r.afterQuotedLiteral(line[i+1:], t)

		case '\\':
			return r.inDoubleQuoteEscape(line[1:i], line[i:], t)
		case '\r':
			return nil, r.syntaxErr("new line in quoted literal")
		case '\n':
			return nil, r.syntaxErr("carriage return in quoted literal")
		}
	}
	return nil, fmt.Errorf("%w: quoted literal not closed", io.ErrUnexpectedEOF)
}

// InSingleQuote continues from "'" in the buffer.
func (r *Reader) inSingleQuote(line []byte, t *Triple) (remainder []byte, err error) {
	// long quote ("'''") option
	if len(line) > 2 && line[1] == '\'' && line[2] == '\'' {
		for i := 3; i < len(line); i++ {
			switch line[i] {
			case '\'': // may terminate
				switch {
				case i+2 >= len(line), line[i+1] != '\'':
					continue
				case line[i+2] != '\'':
					i++
					continue
				}
				t.Object = string(line[3:i])
				return r.afterQuotedLiteral(line[i+3:], t)
			case '\\': // is escape
				return r.longSingleQuote(line[3:i], line[i:], t)
			}
		}
		return r.longSingleQuote(line[3:], nil, t)
	}

	for i := 1; i < len(line); i++ {
		switch line[i] {
		case '\'':
			t.Object = string(line[1:i])
			return r.afterQuotedLiteral(line[i+1:], t)

		case '\\':
			return r.inSingleQuoteEscape(line[1:i], line[i:], t)
		case '\r':
			return nil, r.syntaxErr("new line in quoted literal")
		case '\n':
			return nil, r.syntaxErr("carriage return in quoted literal")
		}
	}
	return nil, fmt.Errorf("%w: quoted literal not closed", io.ErrUnexpectedEOF)
}

func (r *Reader) inDoubleQuoteEscape(copyAsIs, line []byte, t *Triple) (remainder []byte, err error) {
	var b strings.Builder
	// oversized allocation is better than resizes later on
	b.Grow(len(copyAsIs) + len(line) - 4)
	b.Write(copyAsIs)

Escape:
	for {
		line, err := r.inEscape(line, &b)
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(line); i++ {
			switch line[i] {
			case '\\':
				b.Write(line[:i])
				line = line[i:]
				continue Escape

			case '"':
				b.Write(line[:i])
				t.Object = b.String()
				return r.afterQuotedLiteral(line[i+1:], t)

			case '\r':
				return nil, r.syntaxErr("new line in quoted literal")
			case '\n':
				return nil, r.syntaxErr("carriage return in quoted literal")
			}
		}
		return nil, fmt.Errorf("%w: quoted literal not closed", io.ErrUnexpectedEOF)
	}
}

func (r *Reader) inSingleQuoteEscape(copyAsIs, line []byte, t *Triple) (remainder []byte, err error) {
	var b strings.Builder
	// oversized allocation is better than resizes later on
	b.Grow(len(copyAsIs) + len(line) - 4)
	b.Write(copyAsIs)

Escape:
	for {
		line, err := r.inEscape(line, &b)
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(line); i++ {
			switch line[i] {
			case '\\':
				b.Write(line[:i])
				line = line[i:]
				continue Escape

			case '\'':
				b.Write(line[:i])
				t.Object = b.String()
				return r.afterQuotedLiteral(line[i+1:], t)

			case '\r':
				return nil, r.syntaxErr("new line in quoted literal")
			case '\n':
				return nil, r.syntaxErr("carriage return in quoted literal")
			}
		}
		return nil, fmt.Errorf("%w: quoted literal not closed", io.ErrUnexpectedEOF)
	}
}

func (r *Reader) longDoubleQuote(copyAsIs, line []byte, t *Triple) (remainder []byte, err error) {
	var b strings.Builder
	b.Write(copyAsIs)

	for {
		for i := 0; i < len(line); i++ {
			switch line[i] {
			case '\\':
				b.Write(line[:i])
				line, err = r.inEscape(line[i:], &b)
				if err != nil {
					return nil, err
				}
				i = 0

			case '"':
				switch {
				case i+2 >= len(line), line[i+1] != '"':
					continue
				case line[i+2] != '"':
					i++
					continue
				}
				b.Write(line[:i])
				t.Object = b.String()
				return r.afterQuotedLiteral(line[i+3:], t)
			}
		}

		b.Write(line)
		line, err = r.line()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = fmt.Errorf("%w: long quoted literal not closed", io.ErrUnexpectedEOF)
			}
			return nil, err
		}
	}
}

func (r *Reader) longSingleQuote(copyAsIs, line []byte, t *Triple) (remainder []byte, err error) {
	var b strings.Builder
	b.Write(copyAsIs)

	for {
		for i := 0; i < len(line); i++ {
			switch line[i] {
			case '\\':
				b.Write(line[:i])
				line, err = r.inEscape(line[i:], &b)
				if err != nil {
					return nil, err
				}
				i = 0

			case '\'':
				switch {
				case i+2 >= len(line), line[i+1] != '\'':
					continue
				case line[i+2] != '\'':
					i++
					continue
				}
				b.Write(line[:i])
				t.Object = b.String()
				return r.afterQuotedLiteral(line[i+3:], t)
			}
		}

		b.Write(line)
		line, err = r.line()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = fmt.Errorf("%w: long quoted literal not closed", io.ErrUnexpectedEOF)
			}
			return nil, err
		}
	}
}

func (r *Reader) inEscape(line []byte, b *strings.Builder) (remainder []byte, err error) {
	if len(line) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	c := line[0]
	switch c {
	case 'u':
		return r.nHex(line[1:], 4, b)
	case 'U':
		return r.nHex(line[1:], 8, b)

	case 't':
		c = '\t'
	case 'b':
		c = '\b'
	case 'n':
		c = '\n'
	case 'r':
		c = '\r'
	case 'f':
		c = '\f'
	case '"', '\'', '\\':
		break // as is
	}
	b.WriteByte(c)
	return line[1:], nil
}

// NHex decodes a Unicode character of n digits.
func (r *Reader) nHex(line []byte, n int, b *strings.Builder) (remainder []byte, err error) {
	var u uint
	for ; n != 0; n-- {
		if len(line) == 0 {
			return nil, io.ErrUnexpectedEOF
		}

		u <<= 4 // next nible
		switch c := line[0]; {
		case c >= '0' && c <= '9':
			u |= (uint)(c - '0')
		case c >= 'A' && c <= 'F':
			u |= (uint)(c - 'A' + 10)
		case c >= 'a' && c <= 'f':
			u |= (uint)(c - 'a' + 10)
		default:
			return nil, r.syntaxErr("illegal hex in Unicode escape")
		}

		line = line[1:]
	}
	b.WriteRune((rune)(u))
	return line, nil
}

// AfterQuotedLiteral continues with line after a quoted literal was passed.
func (r *Reader) afterQuotedLiteral(line []byte, t *Triple) (remainder []byte, err error) {
	if len(line) != 0 {
		switch line[0] {
		case ' ', '\t', '\r', '\n': // WS
			line = line[1:]

		case '^':
			return r.inDatatype(line, t)

		case '@':
			return r.inLangTag(line, t)
		}
	}

	// “If there is no datatype IRI and no language tag, the datatype is
	// xsd:string.” — W3C Recommendation “RDF 1.1 Turtle”, subsection 2.5.1
	t.DatatypeIRI = XSDString

	return line, nil
}

// InLangTag continues from "@" in the buffer.
func (r *Reader) inLangTag(line []byte, t *Triple) (remainder []byte, err error) {
	// “If the LANGTAG rule matched, the datatype is rdf:langString …”
	// — W3C Recommendation “RDF 1.1 Turtle”, subsection 7.2
	t.DatatypeIRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"

	offset := 1 // pass '@'
	for i := offset; i < len(line); i++ {
		c := line[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' {
			continue
		}
		if c >= '0' && c <= '9' {
			if offset == 1 {
				return nil, r.syntaxErr("decimal in first code of language tag")
			}
			continue
		}

		switch c {
		case '-':
			if offset == i {
				return nil, r.syntaxErr("empty code in language tag")
			}
			offset = i + 1

		case ' ', '\t', '\r', '\n': // WS
			if offset == i {
				return nil, r.syntaxErr("empty code in language tag")
			}
			t.LangTag = string(line[1:i])
			return line[i+1:], nil // ✅

		default:
			return nil, r.syntaxErr("illegal character in language tag")
		}
	}
	return nil, io.ErrUnexpectedEOF
}

// InDatatype continues from "^" in the buffer.
func (r *Reader) inDatatype(line []byte, t *Triple) (remainder []byte, err error) {
	if len(line) < 3 {
		if len(line) < 2 || len(line) < 3 && line[1] == '^' {
			return nil, io.ErrUnexpectedEOF
		}
	}
	if line[1] != '^' {
		return nil, r.syntaxErr(`single "^" after quoted string`)
	}
	if len(line) < 4 {
		return nil, io.ErrUnexpectedEOF
	}
	if line[2] == '<' {
		t.DatatypeIRI, remainder, err = r.inIRI(line[2:])
		return
	}

	var prefixLabel []byte
ReadPrefix:
	for i := 2; ; i++ {
		if i >= len(line) {
			return nil, io.ErrUnexpectedEOF
		}
		switch line[i] {
		case ' ', '\t', '\r', '\n': // WS
			return nil, r.syntaxErr("datatype missing prefix")

		case ':':
			prefixLabel = line[2:i]
			line = line[i+1:]
			break ReadPrefix

		default:
			// TODO: validation
		}
	}

	// allocation omitted by compiler
	prefix, ok := r.prefixPerLabel[string(prefixLabel)]
	if !ok {
		return nil, r.syntaxErr("undefined prefix on datatype")
	}
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ', '\t', '\r', '\n': // WS
			t.DatatypeIRI = prefix + string(line[:i])
			return line[i+1:], nil
		}
		// TODO: validation
	}
	return nil, io.ErrUnexpectedEOF
}
