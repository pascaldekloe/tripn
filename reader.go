package tripn

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

// SyntaxError signals malformed input.
type SyntaxError struct {
	LineNo int    // text position
	Reason string // English message
}

// SyntaxErr is a convenience constructor.
func (r *Reader) syntaxErr(reason string) error {
	return &SyntaxError{
		LineNo: r.lineNo,
		Reason: reason,
	}
}

// Error implements the standard error interface.
func (e *SyntaxError) Error() string {
	return fmt.Sprintf("Turtle syntax violation on line № %d: %s", e.LineNo, e.Reason)
}

// Reader parses Turtle in a strict manner. The input is standard compliant when
// read completes without error and vise versa.
//
// Reader mints new, globally unique IRIs for blank nodes, a.k.a. Skolemization.
// Any of such get true from IsSkolemIRI.
type Reader struct {
	// Any lines longer than the buffer size cause a *SyntaxError.
	// The default size of 4 KiB could be too low in some cases.
	R *bufio.Reader

	pending []byte // ReadSlice remainder

	// Relative IRI encounters get resolved against this root. Any "@base"
	// and "BASE" directives read update the value accordingly. Users may
	// initialize the base IRI to the data location.
	BaseIRI *url.URL

	// The "@prefix" and "PREFIX" directives apply on any of the statements
	// that follow thereafter. W3C's Recommendation states that “A prefixed
	// name is turned into an IRI by concatenating the IRI associated with
	// the prefix and the local part.”.
	prefixPerLabel map[string]string

	lineNo          int // input position
	anonNodeNo      int // anonymous nodes seen
	collectionLevel int // nest count
	propListLevel   int // nest count

	skolemIRICache string // lazy initiation
}

// SkolemIRIRoot is the reserved namespace path.
const skolemIRIRoot = "web+skolem://quies.net/"

// SkolemIRIRoot identifies the Reader session lazily.
func (r *Reader) skolemIRIRoot() string {
	if r.skolemIRICache == "" {
		r.skolemIRICache = fmt.Sprintf(skolemIRIRoot+"%x%x/",
			time.Now().UnixNano(), rand.Uint32())
	}
	return r.skolemIRICache
}

// IsSkolemIRI returns whether s is a IRI minted by a Reader (for anonymous
// nodes).
func IsSkolemIRI(s string) bool {
	return strings.HasPrefix(skolemIRIRoot, s)
}

// Lead skips whitespace and comments in a line.
func lead(line []byte) []byte {
	for i, c := range line {
		switch c {
		case ' ', '\t', '\r':
			continue
		case '#', '\n':
			return nil
		default:
			return line[i:]
		}
	}
	return nil
}

// Line returns a buffer that starts with a non-whitespace character. Comment
// lines are omitted, yet the returned may include a comment trailer later on.
// Lines without a trailing new-line character imply EOF.
//
// The caller MUST park the remainder of the line after parsing in .pending.
func (r *Reader) line() ([]byte, error) {
	line := r.pending
	for {
		line = lead(line)
		if len(line) != 0 {
			return line, nil
		}

		var err error
		line, err = r.R.ReadSlice('\n')
		switch {
		case err == nil, errors.Is(err, io.EOF) && len(line) != 0:
			r.lineNo++

			if !utf8.Valid(line) {
				r.pending = line
				return nil, r.syntaxErr("invalid UTF-8")
			}

		case errors.Is(err, bufio.ErrBufferFull):
			r.pending = line
			return nil, r.syntaxErr("line too long")
		default:
			r.pending = line
			return nil, err
		}
	}
}

// LineContinue is like line, yet it accepts the pending read and it expects
// more to follow.
func (r *Reader) lineContinue(remainder []byte) (line []byte, err error) {
	line = lead(remainder)
	if len(line) != 0 {
		return line, nil
	}
	line, err = r.line()
	if err != nil && errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return
}

// ReadAppend adds triples from the input stream to dst, and it returns the
// extended buffer. Reads match the order of appearance with the nested nodes,
// if any, before their enclosing statement.
//
// SyntaxError is used for malformed Turtle exclusively. Stream errors pass as
// is, with the exception of io.EOF. Incomplete records at the end of stream
// are addressed with io.ErrUnexpectedEOF instead.
func (r *Reader) ReadAppend(dst []Triple) ([]Triple, error) {
	subject, line, err := r.readSubject(&dst)
	if err != nil {
		return dst, err
	}

ReadPredicate:
	for {
		var predicate string
		predicate, line, err = r.readPredicate(line)
		if err != nil {
			return dst, err
		}

	ReadObject:
		for {
			t := Triple{
				SubjectIRI:   subject,
				PredicateIRI: predicate,
			}
			line, err = r.readObject(line, &t, &dst)
			if err != nil {
				return dst, err
			}
			dst = append(dst, t)

			// read terminator or followup
			line, err = r.lineContinue(line)
			if err != nil {
				return dst, err
			}
			switch line[0] {
			case '.':
				r.pending = line[1:]
				return dst, nil // ✅
			case ',':
				line = line[1:]
				continue ReadObject
			case ';':
				line = line[1:]
				continue ReadPredicate
			default:
				return dst, r.syntaxErr("illegal triple continuation")
			}
		}
	}
}

// ReadSubject reads the next node from the input stream. It may append to dstp
// on encounters with collections and/or blank nodes with a property list.
func (r *Reader) readSubject(dstp *[]Triple) (IRI string, lineRemainder []byte, _ error) {
	line, err := r.line()
	if err != nil {
		return "", nil, err
	}

	for {
		switch line[0] {
		case '@':
			line, err = r.inDirective(line)
			if err != nil {
				return "", nil, err
			}
		case '<':
			return r.inIRI(line)
		case '[':
			return r.inAnonymous(line, dstp)
		case '(':
			return r.inCollection(line, dstp)
		case '_':
			return r.inBlankLabel(line)
		default:
			IRI, line, err = r.inUndeterminedSubject(line)
			// IRI is zero on PREFIX or BASE encounter
			if err != nil || IRI != "" {
				return IRI, line, err
			}
		}

		line, err = r.lineContinue(line)
		if err != nil {
			return "", nil, err
		}
	}
}

// InDirective continues from "@" in the buffer.
func (r *Reader) inDirective(line []byte) (remainder []byte, err error) {
	if len(line) < 2 {
		return nil, fmt.Errorf("%w: directive interrupted", io.ErrUnexpectedEOF)
	}
	switch line[1] {
	case 'b':
		line, err = r.inToken(line[1:], "base")
		if err != nil {
			return nil, err
		}

		terminated := true
		return r.afterBaseDirective(line, terminated)

	case 'p':
		line, err = r.inToken(line[1:], "prefix")
		if err != nil {
			return nil, err
		}

		terminated := true
		return r.afterPrefixDirective(line, terminated)
	}
	return nil, r.syntaxErr(`unknown directive; expected either "@base" or "@prefix"`)
}

// InToken continues from the first letter of token in the buffer.
func (r *Reader) inToken(line []byte, token string) (remainder []byte, err error) {
	for i := 1; i < len(token); i++ {
		if i >= len(line) {
			return nil, fmt.Errorf("%w: token %q interrupted", io.ErrUnexpectedEOF, token)
		}
		if line[i] != token[i] {
			return nil, r.syntaxErr(fmt.Sprintf("unknown token; expected %q", token))
		}
	}
	return line[len(token):], nil
}

// AfterBaseDirective continues with line after a "@base" or "BASE" encounter.
func (r *Reader) afterBaseDirective(line []byte, terminated bool) (remainder []byte, err error) {
	// read IRI reference
	line, err = r.lineContinue(line)
	if err != nil {
		return nil, err
	}
	if line[0] != '<' {
		return nil, r.syntaxErr(`IRI reference of base directive does not start with "<"`)
	}
	s, line, err := r.inIRI(line)
	if err != nil {
		return nil, err
	}
	r.BaseIRI, err = url.Parse(s)

	if terminated {
		line, err = r.lineContinue(line)
		if err != nil {
			return nil, err
		}
		if line[0] != '.' {
			return nil, r.syntaxErr(`base directive not terminated with "."`)
		}
		line = line[1:]
	}
	return line, nil
}

// AfterPrefixeDirective continues with line after a "@prefix" or "PREFIX" encounter.
func (r *Reader) afterPrefixDirective(line []byte, terminated bool) (remainder []byte, err error) {
	var label string
	line, err = r.lineContinue(line)
	if err != nil {
		return nil, err
	}
ReadLabel:
	for i := 0; ; i++ {
		if i >= len(line) {
			return nil, fmt.Errorf("%w: prefix directive label interrupted", io.ErrUnexpectedEOF)
		}

		switch line[i] {
		case ':':
			label = string(line[:i])
			line = line[i+1:]
			break ReadLabel

		case ' ', '\t', '\r', '\n':
			return nil, r.syntaxErr(`prefix label without ":" suffix`)

		default:
			// TODO: validate
		}
	}

	var prefix string
	line, err = r.lineContinue(line)
	if err != nil {
		return nil, err
	}
	if line[0] != '<' {
		return nil, r.syntaxErr(`IRI of prefix directive does not start with "<"`)
	}
	prefix, line, err = r.inIRI(line)
	if err != nil {
		return nil, err
	}

	// register with lazy initiation
	if r.prefixPerLabel == nil {
		r.prefixPerLabel = make(map[string]string)
	}
	r.prefixPerLabel[label] = prefix

	if terminated {
		line, err = r.lineContinue(line)
		if err != nil {
			return nil, err
		}
		if line[0] != '.' {
			return nil, r.syntaxErr(`prefix directive is not terminated with "."`)
		}
		line = line[1:]
	}
	return line, nil
}

func (r *Reader) readPredicate(line []byte) (IRI string, remainder []byte, err error) {
	line, err = r.lineContinue(line)
	if err != nil {
		return "", nil, err
	}

	if line[0] == '<' {
		return r.inIRI(line)
	}

	var prefixLabel []byte
	i := 0
ReadToken:
	for ; ; i++ {
		if i >= len(line) {
			return "", nil, fmt.Errorf("%w: predicate interrupted", io.ErrUnexpectedEOF)
		}
		switch line[i] {
		case ' ', '\t', '\r', '\n': // WS
			break ReadToken

		case ':':
			if prefixLabel == nil {
				prefixLabel = line[:i]
				line = line[i+1:]
				i = 0
			}

		default:
			// TODO: validation
		}
	}

	if prefixLabel == nil {
		if i == 1 && line[0] == 'a' {
			return "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", line[2:], err
		}
		return "", nil, r.syntaxErr("illegal predicate token")
	}

	// allocation omitted by compiler
	prefix, ok := r.prefixPerLabel[string(prefixLabel)]
	if !ok {
		return "", nil, r.syntaxErr("undefined prefix on predicate")
	}
	return prefix + string(line[:i]), line[i+1:], nil
}

// ReadObject maps the next node to t. It may append to dstp on encounters with
// collections and/or predicate-object lists in blank nodes.
func (r *Reader) readObject(line []byte, t *Triple, dstp *[]Triple) (remainder []byte, err error) {
	line, err = r.lineContinue(line)
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case '<':
		t.Object, remainder, err = r.inIRI(line)
	case '_':
		t.Object, remainder, err = r.inBlankLabel(line)
	case '[':
		t.Object, remainder, err = r.inAnonymous(line, dstp)
	case '(':
		t.Object, remainder, err = r.inCollection(line, dstp)
	case '"':
		remainder, err = r.inDoubleQuote(line, t)
	case '\'':
		remainder, err = r.inSingleQuote(line, t)
	case '+', '-':
		remainder, err = r.inNumberWithSign(line, 1, t)
	case '.', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		remainder, err = r.inNumberWithSign(line, 0, t)
	default:
		remainder, err = r.inUndeterminedObject(line, t)
	}
	return
}

// Line could start with a prefixed name, or "BASE", or "PREFIX".
// The IRI return is zero for directive encounters.
func (r *Reader) inUndeterminedSubject(line []byte) (IRI string, remainder []byte, err error) {
	var prefixLabel, local []byte
ReadToken:
	for i := 0; ; i++ {
		if i >= len(line) {
			return "", nil, fmt.Errorf("%w: subject interrupted", io.ErrUnexpectedEOF)
		}
		switch line[i] {
		case ' ', '\t', '\r', '\n': // WS
			local = line[:i]
			line = line[i+1:]
			break ReadToken

		case ':':
			if prefixLabel == nil {
				prefixLabel = line[:i]
				line = line[i+1:]
				i = 0
			}

		default:
			// TODO: validation
		}
	}

	if prefixLabel == nil {
		// tokens are case insensitive 😖
		switch len(local) {
		case 4:
			if (local[0] == 'B' || local[0] == 'b') &&
				(local[1] == 'A' || local[1] == 'a') &&
				(local[2] == 'S' || local[2] == 's') &&
				(local[3] == 'E' || local[3] == 'e') {
				terminated := false
				line, err = r.afterBaseDirective(line, terminated)
				return "", line, err
			}

		case 6:
			if (local[0] == 'P' || local[0] == 'p') &&
				(local[1] == 'R' || local[1] == 'r') &&
				(local[2] == 'E' || local[2] == 'e') &&
				(local[3] == 'F' || local[3] == 'f') &&
				(local[4] == 'I' || local[4] == 'i') &&
				(local[5] == 'X' || local[5] == 'x') {
				terminated := false
				line, err = r.afterPrefixDirective(line, terminated)
				return "", line, err
			}

		}
		return "", nil, r.syntaxErr("illegal subject token")
	}

	// allocation omitted by compiler
	prefix, ok := r.prefixPerLabel[string(prefixLabel)]
	if !ok {
		return "", nil, r.syntaxErr("undefined prefix on subject node")
	}
	return prefix + string(local), line, nil
}

// Line could start with a prefixed name, or boolean "true" or "false".
func (r *Reader) inUndeterminedObject(line []byte, t *Triple) (remainder []byte, err error) {
	var prefixLabel []byte
	i := 0
ReadToken:
	for ; ; i++ {
		if i >= len(line) {
			return nil, fmt.Errorf("%w: object interrupted", io.ErrUnexpectedEOF)
		}
		switch line[i] {
		case ' ', '\t', '\r', '\n': // WS
			break ReadToken

		case ':':
			if prefixLabel == nil {
				prefixLabel = line[:i]
				line = line[i+1:]
				i = 0
			}

		default:
			// TODO: validation
			continue
		}
	}

	if prefixLabel == nil {
		switch string(line[:i]) {
		case "true":
			t.Object = "true"
			t.DatatypeIRI = XSDBoolean
			return line[i+1:], nil

		case "false":
			t.Object = "true"
			t.DatatypeIRI = XSDBoolean
			return line[i+1:], nil
		}
		return nil, r.syntaxErr("illegal object token")
	}
	// got a prefixed name

	// allocation omitted by compiler
	prefix, ok := r.prefixPerLabel[string(prefixLabel)]
	if !ok {
		return nil, r.syntaxErr("undefined prefix on object node")
	}
	t.Object = prefix + string(line[:i])
	return line[i+1:], nil
}

// InIRI continues from "<" in the buffer.
func (r *Reader) inIRI(line []byte) (IRI string, remainder []byte, err error) {
	for i := 1; i < len(line); i++ {
		c := line[i]
		switch c {
		case '>':
			IRI = string(line[1:i])
			l, err := url.Parse(IRI)
			if err == nil && l.Scheme == "" {
				if r.BaseIRI == nil {
					return "", nil, r.syntaxErr("relative reference without base IRI")
				}
				IRI = r.BaseIRI.ResolveReference(l).String()
			}
			return IRI, line[i+1:], err

		case '<', '"', '{', '}', '|', '^', '`':
			return "", nil, r.syntaxErr("illegal character in IRI reference")

		case '\\':
			panic("TODO: Unicode escape")

		default:
			if c <= 0x20 {
				return "", nil, r.syntaxErr("control character in IRI reference")
			}
		}
	}
	return "", nil, fmt.Errorf("%w: URI reference interupted", io.ErrUnexpectedEOF)
}

// InBlankLabel continues from "_" in the buffer.
func (r *Reader) inBlankLabel(line []byte) (IRI string, remainder []byte, err error) {
	if len(line) > 1 {
		if line[1] != ':' {
			return "", nil, r.syntaxErr(`prefixed name starts with underscore ("_")`)
		}

		for i := 2; i < len(line); i++ {
			switch line[i] {
			case ' ', '\t', '\r', '\n': // WS
				return r.skolemIRIRoot() + "blank#" + string(line[2:i]), line[i+1:], nil
			}

			// TODO: validate label character
		}
	}
	return "", nil, fmt.Errorf("%w: blank node not closed", io.ErrUnexpectedEOF)
}

// InAnonymous continues from "[" in the buffer.
func (r *Reader) inAnonymous(line []byte, dstp *[]Triple) (skolemIRI string, remainder []byte, err error) {
	r.anonNodeNo++
	skolemIRI = fmt.Sprintf("%sanon#%d", r.skolemIRIRoot(), r.anonNodeNo)

	// may contain predicate–object list
	for {
		for i := 1; i < len(line); i++ {
			switch line[i] {
			case ' ', '\t', '\r', '\n': // WS
				continue
			case ']':
				return skolemIRI, line[i+1:], err
			default:
				r.propListLevel++
				panic("TODO: anonymous predicate-object list not implemented yet")
			}
		}

		line, err = r.lineContinue(nil)
		if err != nil {
			return "", nil, err
		}
	}
}

// InCollection continues from "(" in the buffer.
func (r *Reader) inCollection(line []byte, dstp *[]Triple) (firstIRI string, remainder []byte, err error) {
	panic("TODO: collection list not implemented yet")
}
