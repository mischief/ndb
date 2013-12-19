// Package ndb implements the Network Database described in
// http://plan9.bell-labs.com/magic/man2html/6/ndb.
package ndb

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// Default NDB file
	NdbLocal = "/lib/ndb/local"
)

// A single database attribute=value tuple.
// The value may be empty.
type Tuple struct {
	Attr, Val string
}

// A NDB record, which may contain multiple tuples,
// and may span multiple lines in the file.
type Record []Tuple

// RecordSet is a related group of records, from a single entry in ndb.
type RecordSet []Record

// Search a RecordSet for a given attribute and return the value.
// Returns "" if not present.
func (r RecordSet) Search(attr string) string {
	for _, rec := range r {
		for _, tuple := range rec {
			if tuple.Attr == attr {
				return tuple.Val
			}
		}
	}

	return ""
}

// Ndb possibly comprised of multiple files.
type Ndb struct {
	filename string        // NDB file name
	data     *bytes.Reader // Raw data
	mtime    time.Time     // Last modified time
	records  RecordSet     // NDB Records
	next     *Ndb          // Next in linked list
}

// Open an NDB database file.
func Open(fname string) (*Ndb, error) {
	var db, first, last *Ndb
	var err error

	if fname == "" {
		fname = NdbLocal
	}
	db, err = openone(fname)
	if err != nil {
		return nil, err
	}

	first = db
	last = db

	// open other db files
	if dbrec := db.Search("database", ""); dbrec != nil {

		for _, files := range dbrec[0] {
			if files.Attr == "file" {
				if files.Val == fname {
					if first.next == nil {
						continue
					}
					if first.filename == fname {
						db = first
						first = first.next
						last.next = db
						last = db
					}
					continue
				}
				if db, err = openone(files.Val); err != nil {
					return nil, err
				}
				last.next = db
				last = db
			}
		}
	}

	return first, nil
}

// Open just one NDB file
func openone(fname string) (*Ndb, error) {
	db := &Ndb{filename: fname}

	// open file
	f, err := os.Open(db.filename)

	if err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}

	defer f.Close()

	// read mtime
	if fstat, err := f.Stat(); err != nil {
		return nil, fmt.Errorf("open: %s", err)
	} else {
		db.mtime = fstat.ModTime()
	}

	// read all data
	if data, err := ioutil.ReadAll(f); err != nil {
		return nil, fmt.Errorf("open: %s", err)
	} else {
		db.data = bytes.NewReader(data)
	}

	// parse records
	if db.records, err = parserec(db); err != nil {
		return nil, fmt.Errorf("open: %s", err)
	}

	return db, nil
}

// Reopen NDB file.
func (n *Ndb) Reopen() error {
	for db := n; db != nil; db = db.next {
		if newdb, err := openone(db.filename); err != nil {
			return err
		} else {
			db.data = newdb.data
			db.mtime = newdb.mtime
			db.records = newdb.records
		}
	}

	return nil
}

// Check if any db files changed.
func (n *Ndb) Changed() (bool, error) {
	for db := n; db != nil; db = db.next {
		fi, err := os.Stat(db.filename)
		if err != nil {
			return false, err
		}

		if db.mtime != fi.ModTime() {
			return true, nil
		}
	}

	return false, nil
}

// Search for a record set with the given attr=val.
// Returns no records (nil) if not found.
func (n *Ndb) Search(attr, val string) RecordSet {
	var results RecordSet

	// check each db file
	for db := n; db != nil; db = db.next {

		// and check each record
		for _, record := range db.records {

			// each each tuple!
			for _, tuple := range record {
				// if val is "" we don't care what it is
				if val == "" && tuple.Attr == attr {
					results = append(results, record)
				} else if tuple.Attr == attr && tuple.Val == val {
					results = append(results, record)
				}
			}
		}

	}

	return results
}

// Parse whole ndb records from the ndb
func parserec(n *Ndb) (RecordSet, error) {
	var err error

	records := make(RecordSet, 1)

	n.data.Seek(0, 0)

	scanl := bufio.NewScanner(n.data)

	var rec Record

	for scanl.Scan() {
		line := scanl.Text()

		// skip empty lines
		if line == "" {
			continue
		}

		first, _ := utf8.DecodeRuneInString(line)

		// comment, skip
		if first == '#' {
			continue
		}

		// not whitespace, begin a record
		if !unicode.IsSpace(first) {
			records = append(records, rec)
			rec = Record{}
		}

		if tuples, terr := parsetuples(line); err != nil {
			err = terr
			break
		} else {
			rec = append(rec, tuples...)
		}

	}

	if err := scanl.Err(); err != nil {
		return nil, err
	}

	// make sure to get the last record.
	records = append(records, rec)

	return records, err
}

// bufio.Scanner function to split data by words and quoted strings
func scanStrings(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Skip leading spaces.
	start := 0
	for width := 0; start < len(data); start += width {
		var r rune
		r, width = utf8.DecodeRune(data[start:])
		if !unicode.IsSpace(r) {
			break
		}
	}

	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// Scan until space, marking end of word.
	inquote := false
	for width, i := 0, start; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		if r == '"' {
			inquote = !inquote
			continue
		}
		if unicode.IsSpace(r) && !inquote {
			return i + width, data[start:i], nil
		}
	}
	// If we're at EOF, we have a final, non-empty, non-terminated word. Return it.
	if atEOF && len(data) > start {
		return len(data), data[start:], nil
	}
	// Request more data.
	return 0, nil, nil
}

// split up a string into ndb tuples.
// parse "quoted strings" correctly, and
// ignore comments at end of line
func parsetuples(line string) ([]Tuple, error) {
	tuples := make([]Tuple, 0)

	// only chop comment if it is at the beginning of a line
	// TODO: make comments work anywhere not in quotes
	if line[0] == '#' {
		return tuples, nil
	}

	scanw := bufio.NewScanner(strings.NewReader(line))
	scanw.Split(scanStrings)

	for scanw.Scan() {
		tpstr := scanw.Text()
		//fmt.Printf("tuple %q\n", tpstr)
		spl := strings.SplitN(tpstr, "=", 2)

		if len(spl) != 2 {
			return nil, fmt.Errorf("invalid tuple %q", tpstr)
		}

		spl[1] = strings.TrimLeft(spl[1], `"`)
		spl[1] = strings.TrimRight(spl[1], `"`)

		tuples = append(tuples, Tuple{spl[0], spl[1]})
	}

	return tuples, nil
}
