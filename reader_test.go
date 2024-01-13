package tripn

import (
	"bufio"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
)

var turtleTriples = []struct {
	turtle  string
	triples []Triple
}{
	// allow empty
	{"", []Triple{}},
	{"\n", []Triple{}},
	{"\r\n\r", []Triple{}},
	{`	# leading and trailing whitespace
 `,
		[]Triple{},
	},
	{`# header
# EOF at comment end`,
		[]Triple{},
	},

	{`<http://example.com/subject1> # N-Triples notation
<http://example.com/predicate1>         # stretched over multiple lines
# with leading and trailing space:

 <http://example.com/object1> 
	. `,
		[]Triple{
			{"http://example.com/subject1", "http://example.com/predicate1", "http://example.com/object1", "", ""},
		},
	},

	{`@base <http://example.com/> . # directive with dot terminator
<subject1> <predicate1> <object1> .
BASE <http://example.net/>              # SPARQL variant without dot
<subject2> <predicate2> <object2> .`,
		[]Triple{
			{"http://example.com/subject1", "http://example.com/predicate1", "http://example.com/object1", "", ""},
			{"http://example.net/subject2", "http://example.net/predicate2", "http://example.net/object2", "", ""},
		},
	},
	{` base <http://example.com/> <subject1> <predicate1> <object1> .
	   @base <http://example.net/> . <subject2> <predicate2> <object2> .
# uncommon yet legal`,
		[]Triple{
			{"http://example.com/subject1", "http://example.com/predicate1", "http://example.com/object1", "", ""},
			{"http://example.net/subject2", "http://example.net/predicate2", "http://example.net/object2", "", ""},
		},
	},

	{`bASe <http://example.com/> @prefix p: <path/> . p:subject1 p:predicate1 p:object1 .`,
		[]Triple{
			{"http://example.com/path/subject1", "http://example.com/path/predicate1", "http://example.com/path/object1", "", ""},
		},
	},
	{`@base <http://example.com/> . PrefiX p: <path/> p:subject1 p:predicate1 p:object1 .`,
		[]Triple{
			{"http://example.com/path/subject1", "http://example.com/path/predicate1", "http://example.com/path/object1", "", ""},
		},
	},

	{`@prefix : <http://example.com/> .   # empty prefix
          :subject1 :predicate1 :object1 .
          :subject2 a :object2 .              # rdf:type predicate`,
		[]Triple{
			{"http://example.com/subject1", "http://example.com/predicate1", "http://example.com/object1", "", ""},
			{"http://example.com/subject2", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://example.com/object2", "", ""},
		},
	},

	{`<http://伝言.example.com/?user=أكرم&amp;channel=R%26D> a true .`,
		[]Triple{
			{"http://伝言.example.com/?user=أكرم&amp;channel=R%26D", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
				"true", "http://www.w3.org/2001/XMLSchema#boolean", ""},
		},
	},

	// predicate list
	{`<http://example.org/#spiderman> <http://www.perceive.net/schemas/relationship/enemyOf> <http://example.org/#green-goblin> ;
                                             <http://xmlns.com/foaf/0.1/name> "Spiderman" .`,
		[]Triple{
			{"http://example.org/#spiderman", "http://www.perceive.net/schemas/relationship/enemyOf", "http://example.org/#green-goblin", "", ""},
			{"http://example.org/#spiderman", "http://xmlns.com/foaf/0.1/name", "Spiderman", "http://www.w3.org/2001/XMLSchema#string", ""},
		},
	},

	// object list with plain string and localized variant
	{`<http://example.org/#spiderman> <http://xmlns.com/foaf/0.1/name> "Spiderman", "Человек-паук"@ru .`,
		[]Triple{
			{"http://example.org/#spiderman", "http://xmlns.com/foaf/0.1/name", "Spiderman",
				"http://www.w3.org/2001/XMLSchema#string", ""},
			{"http://example.org/#spiderman", "http://xmlns.com/foaf/0.1/name", "Человек-паук",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "ru"},
		},
	},

	// EXAMPLE 1 from W3C's “RDF 1.1 Turtle” Recommendation
	{
		`@base <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rel: <http://www.perceive.net/schemas/relationship/> .

<#green-goblin>
    rel:enemyOf <#spiderman> ;
    a foaf:Person ;    # in the context of the Marvel universe
    foaf:name "Green Goblin" .

<#spiderman>
    rel:enemyOf <#green-goblin> ;
    a foaf:Person ;
    foaf:name "Spiderman", "Человек-паук"@ru .`,
		[]Triple{{
			"http://example.org/#green-goblin",
			"http://www.perceive.net/schemas/relationship/enemyOf",
			"http://example.org/#spiderman", "", "",
		}, {
			"http://example.org/#green-goblin",
			"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
			"http://xmlns.com/foaf/0.1/Person", "", "",
		}, {
			"http://example.org/#green-goblin",
			"http://xmlns.com/foaf/0.1/name",
			"Green Goblin", "http://www.w3.org/2001/XMLSchema#string", "",
		}, {
			"http://example.org/#spiderman",
			"http://www.perceive.net/schemas/relationship/enemyOf",
			"http://example.org/#green-goblin", "", "",
		}, {
			"http://example.org/#spiderman",
			"http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
			"http://xmlns.com/foaf/0.1/Person", "", "",
		}, {
			"http://example.org/#spiderman",
			"http://xmlns.com/foaf/0.1/name",
			"Spiderman", "http://www.w3.org/2001/XMLSchema#string", "",
		}, {
			"http://example.org/#spiderman",
			"http://xmlns.com/foaf/0.1/name",
			"Человек-паук", "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "ru",
		}},
	},

	// quoted strings EXAMPLE 11 from W3C's “RDF 1.1 Turtle” Recommendation
	{
		`@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix show: <http://example.org/vocab/show/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

show:218 rdfs:label "That Seventies Show"^^xsd:string .            # literal with XML Schema string datatype
show:218 rdfs:label "That Seventies Show"^^<http://www.w3.org/2001/XMLSchema#string> . # same as above
show:218 rdfs:label "That Seventies Show" .                                            # same again
show:218 show:localName "That Seventies Show"@en .                 # literal with a language tag
show:218 show:localName 'Cette Série des Années Soixante-dix'@fr . # literal delimited by single quote
show:218 show:localName "Cette Série des Années Septante"@fr-be .  # literal with a region subtag
show:218 show:blurb '''This is a multi-line                        # literal with embedded new lines and quotes
literal with many quotes (""""")
and up to two sequential apostrophes ('').''' .
`,
		[]Triple{
			{"http://example.org/vocab/show/218", "http://www.w3.org/2000/01/rdf-schema#label",
				"That Seventies Show", "http://www.w3.org/2001/XMLSchema#string", ""},
			{"http://example.org/vocab/show/218", "http://www.w3.org/2000/01/rdf-schema#label",
				"That Seventies Show", "http://www.w3.org/2001/XMLSchema#string", ""},
			{"http://example.org/vocab/show/218", "http://www.w3.org/2000/01/rdf-schema#label",
				"That Seventies Show", "http://www.w3.org/2001/XMLSchema#string", ""},
			{"http://example.org/vocab/show/218", "http://example.org/vocab/show/localName",
				"That Seventies Show",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "en"},
			{"http://example.org/vocab/show/218", "http://example.org/vocab/show/localName",
				"Cette Série des Années Soixante-dix",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "fr"},
			{"http://example.org/vocab/show/218", "http://example.org/vocab/show/localName",
				"Cette Série des Années Septante",
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#langString", "fr-be"},
			{"http://example.org/vocab/show/218", "http://example.org/vocab/show/blurb",
				`This is a multi-line                        # literal with embedded new lines and quotes
literal with many quotes (""""")
and up to two sequential apostrophes ('').`,
				"http://www.w3.org/2001/XMLSchema#string", ""},
		},
	},

	// numbers EXAMPLE 12 from W3C's “RDF 1.1 Turtle” Recommendation
	{`@prefix : <http://example.org/elements/> .
 <http://en.wikipedia.org/wiki/Helium>
    :atomicNumber 2 ;               # xsd:integer
    :atomicMass 4.002602 ;          # xsd:decimal
    :specificGravity 1.663E-4 .     # xsd:double
`,
		[]Triple{
			{"http://en.wikipedia.org/wiki/Helium", "http://example.org/elements/atomicNumber",
				"2", "http://www.w3.org/2001/XMLSchema#integer", ""},
			{"http://en.wikipedia.org/wiki/Helium", "http://example.org/elements/atomicMass",
				"4.002602", "http://www.w3.org/2001/XMLSchema#decimal", ""},
			{"http://en.wikipedia.org/wiki/Helium", "http://example.org/elements/specificGravity",
				"1.663E-4", "http://www.w3.org/2001/XMLSchema#double", ""},
		},
	},

	// blank nodes EXAMPLE 14 from W3C's “RDF 1.1 Turtle” Recommendation
	{`@prefix foaf: <http://xmlns.com/foaf/0.1/> .

_:alice foaf:knows _:bob .
_:bob foaf:knows _:alice .`,
		[]Triple{
			{"http://example.com/skolem-stub/blank#alice", "http://xmlns.com/foaf/0.1/knows",
				"http://example.com/skolem-stub/blank#bob", "", ""},
			{"http://example.com/skolem-stub/blank#bob", "http://xmlns.com/foaf/0.1/knows",
				"http://example.com/skolem-stub/blank#alice", "", ""},
		},
	},
}

func TestReader(t *testing.T) {
	for _, test := range turtleTriples {
		// sample stream
		r := Reader{
			R:              bufio.NewReader(strings.NewReader(test.turtle)),
			skolemIRICache: "http://example.com/skolem-stub/",
		}

		var got []Triple
	ReadSample:
		for {
			var err error
			got, err = r.ReadAppend(got)
			switch err {
			case nil:
				continue
			case io.EOF:
				break ReadSample
			}

			var msg strings.Builder
			fmt.Fprintf(&msg, "read error: %s\n… for turtle:", err)

			// dump with line numbers and CR/LF visualization
			s := test.turtle
			lineNo := 1
			for ; ; lineNo++ {
				before, after, found := strings.Cut(s, "\n")
				if !found {
					break
				}
				fmt.Fprintf(&msg, "\n%02d: %s⏎", lineNo,
					strings.ReplaceAll(before, "\r", "␍"))
				s = after
			}
			if s != "" {
				fmt.Fprintf(&msg, "\n%02d: %s", lineNo,
					strings.ReplaceAll(s, "\r", "␍"))
			}
			t.Fatal(msg.String())
		}

		if slices.Equal(got, test.triples) {
			continue // test passed
		}
		// fail test
		msg := "got triples:"
		for _, t := range got {
			msg += "\n\t" + t.String()
		}
		msg += "\nwant triples:"
		for _, t := range test.triples {
			msg += "\n\t" + t.String()
		}
		t.Error(msg, "\nfor Turtle:\n", test.turtle)
	}
}
