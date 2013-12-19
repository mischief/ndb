package ndb

import (
	"bytes"
	"io/ioutil"
	"testing"
)

const (
	testndb = "testndb/local"
)

type NdbParseTest struct {
	line string

	ntup   int
	tuples []Tuple
}

var (
	parsetests = []NdbParseTest{
		NdbParseTest{
			line: `irc= host=irc.foo.org port=6697 ssl=true
  nick=gordon user=gordon real=gordon
  channels="#foo #bar"

wtmp= file=/var/log/wtmp
mailwatch= dir=/home/mischief/Maildir
markov= order=2 nword=30 corpus=data/corpus`,
			ntup:   16,
			tuples: []Tuple{Tuple{"irc", ""}, Tuple{"host", "irc.foo.org"}},
		},
		NdbParseTest{
			line:   `one=one`,
			ntup:   1,
			tuples: []Tuple{Tuple{"one", "one"}},
		},
	}
)

func TestParseTuples(t *testing.T) {

	for tno, test := range parsetests {
		tup, err := parsetuples(test.line)

		t.Logf("%q -> %+v", test.line, tup)

		if err != nil {
			t.Error(err)
			continue
		}

		if len(tup) != test.ntup {
			t.Errorf("test %d: expected %d records got %d", tno, test.ntup, len(tup))
			continue
		}

		for n, tuple := range test.tuples {
			if tup[n].Attr != tuple.Attr {
				t.Errorf("test %d: tuple %d: expected attr %q got %q", tno, n, tuple.Attr, tup[n].Attr)
			}

			if tup[n].Val != tuple.Val {
				t.Errorf("test %d: tuple %d: expected val %q got %q", tno, n, tuple.Val, tup[n].Val)
			}
		}
	}

	//t.Logf("%+v", tup)
}

func TestParseRecord(t *testing.T) {
	data, err := ioutil.ReadFile(testndb)

	if err != nil {
		t.Fatal(err)
	}

	ndb := &Ndb{data: bytes.NewReader(data)}
	rec, err := parserec(ndb)

	if err != nil {
		t.Fatal(err)
	}

	for _, record := range rec {

		for n, tuple := range record {
			if n == 0 {
				t.Logf("%+v", tuple)
			} else {
				t.Logf("  %+v", tuple)
			}
		}
	}
}

func TestNdbOpen(t *testing.T) {
	ndb, err := Open(testndb)

	if err != nil {
		t.Fatal(err)
	}

	dbfs := []string{"testndb/local", "testndb/common"}

	for _, dbf := range dbfs {
		if ndb.filename != dbf {
			t.Fatalf("wrong db file: expected %q got %q", dbf, ndb.filename)
		}
		ndb = ndb.next
	}

}

func TestNdbSearch(t *testing.T) {
	ndb, err := Open(testndb)

	if err != nil {
		t.Fatal(err)
	}

	attr := "tcp"

	recs := ndb.Search(attr, "")

	if recs == nil || len(recs) == 0 {
		t.Fatalf("search for %q failed", attr)
	}

	for _, rec := range recs {
		t.Logf("record %+v:", rec[0])
		for _, tuple := range rec[1:] {
			t.Logf("  %+v", tuple)
		}
	}

	attr = "udp"
	recs = ndb.Search(attr, "syslog")

	if syslog := recs.Search("port"); syslog != "514" {
		t.Fatalf("expected 514, got %q", syslog)
	}
}
