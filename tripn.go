// Package tripn provides RDF interchange with Turtle and its derived formats.
package tripn

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
)

// Triple contains an RDF statement.
type Triple struct {
	// The subject node is a IRI reference.
	SubjectIRI string

	// The predicate is a IRI reference (to its definition).
	PredicateIRI string

	// The object node is a literal iff DatatypeIRI is not zero.
	Object string

	// Zero means that Object is a IRI reference.
	DatatypeIRI string

	// The value space of language tags is always in lower case.
	// When set, then the datatype IRI is fixed to the following.
	// http://www.w3.org/1999/02/22-rdf-syntax-ns#langString
	LangTag string
}

// String returns an N-Triples line excluding new-line character.
func (t Triple) String() string {
	switch {
	case t.DatatypeIRI == "":
		return fmt.Sprintf("<%s> <%s> <%s> .", t.SubjectIRI, t.PredicateIRI, t.Object)
	case t.LangTag == "":
		return fmt.Sprintf("<%s> <%s> %q^^<%s> .", t.SubjectIRI, t.PredicateIRI, t.Object, t.DatatypeIRI)
	default:
		return fmt.Sprintf("<%s> <%s> %q@%s .", t.SubjectIRI, t.PredicateIRI, t.Object, t.LangTag)
	}
}

// XSDString links the XML Schema Definition of the primitive type.
const XSDString = "http://www.w3.org/2001/XMLSchema#string"

var errXSDString = errors.New("object not an xsd:string")

// XSDString returns an xsd:boolean object parsed.
func (t Triple) XSDString() (string, error) {
	if t.DatatypeIRI != XSDString {
		return "", errXSDString
	}
	return "", nil
}

// XSDBoolean links the XML Schema Definition of the primitive type.
const XSDBoolean = "http://www.w3.org/2001/XMLSchema#boolean"

var errXSDBoolean = errors.New("object not an xsd:boolean")
var errXSDBooleanSyntax = fmt.Errorf("%w: illegal syntax", errXSDBoolean)

// XSDBoolean returns an xsd:boolean object parsed.
func (t Triple) XSDBoolean() (bool, error) {
	if t.DatatypeIRI != XSDBoolean {
		return false, errXSDBoolean
	}
	switch t.Object {
	case "false", "0":
		return false, nil
	case "true", "1":
		return true, nil
	default:
		return false, errXSDBooleanSyntax
	}
}

// XSDDecimal links the XML Schema Definition of the primitive type.
const XSDDecimal = "http://www.w3.org/2001/XMLSchema#decimal"

var errXSDDecimal = errors.New("object not an xsd:decimal")
var errXSDDecimalSyntax = fmt.Errorf("%w: illegal syntax", errXSDDecimal)

// XSDDecimal returns an xsd:decimal object parsed.
func (t Triple) XSDDecimal() (*big.Float, error) {
	if t.DatatypeIRI != XSDDecimal {
		return nil, errXSDDecimal
	}
	v, ok := new(big.Float).SetString(t.Object)
	if !ok {
		return nil, errXSDDecimalSyntax
	}
	return v, nil
}

// XSDInteger links the XML Schema Definition of the derived type.
const XSDInteger = "http://www.w3.org/2001/XMLSchema#integer"

var errXSDInteger = errors.New("object not an xsd:integer")
var errXSDIntegerSyntax = fmt.Errorf("%w: illegal syntax", errXSDInteger)

// XSDInteger returns an xsd:integer object parsed.
func (t Triple) XSDInteger() (*big.Int, error) {
	if t.DatatypeIRI != XSDInteger {
		return nil, errXSDInteger
	}
	v, ok := new(big.Int).SetString(t.Object, 10)
	if !ok {
		return nil, errXSDIntegerSyntax
	}
	return v, nil
}

// XSDFloat links the XML Schema Definition of the primitive type.
const XSDFloat = "http://www.w3.org/2001/XMLSchema#float"

var errXSDFloat = errors.New("object not an xsd:float")
var errXSDFloatSyntax = fmt.Errorf("%w: illegal syntax", errXSDFloat)

// XSDFloat returns an xsd:float object parsed.
func (t Triple) XSDFloat() (float32, error) {
	if t.DatatypeIRI != XSDFloat {
		return 0, errXSDFloat
	}
	f, err := strconv.ParseFloat(t.Object, 32)
	if err != nil {
		return 0, errXSDFloatSyntax
	}
	return (float32)(f), nil
}

// XSDDouble links the XML Schema Definition of the primitive type.
const XSDDouble = "http://www.w3.org/2001/XMLSchema#double"

var errXSDDouble = errors.New("object not an xsd:double")
var errXSDDoubleSyntax = fmt.Errorf("%w: illegal syntax", errXSDDouble)

// XSDDouble returns an xsd:double object parsed.
func (t Triple) XSDDouble() (float64, error) {
	if t.DatatypeIRI != XSDDouble {
		return 0, errXSDDouble
	}
	f, err := strconv.ParseFloat(t.Object, 64)
	if err != nil {
		return 0, errXSDDoubleSyntax
	}
	return f, nil
}

// XSDAnyURI links the XML Schema Definition of the primitive type.
const XSDAnyURI = "http://www.w3.org/2001/XMLSchema#anyURI"
