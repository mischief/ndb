package main

import (
	"flag"
	"fmt"
	"github.com/mischief/ndb"
	"os"
)

var (
	ndbfile = flag.String("f", ndb.NdbLocal, "ndb file")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [-f ndbfile] attr val [rattr]\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	narg := flag.NArg()

	if narg < 2 || narg > 3 {
		usage()
		os.Exit(1)
	}

	db, err := ndb.Open(*ndbfile)

	if err != nil {
		fmt.Fprint(os.Stderr, err)
		os.Exit(1)
	}

	records := db.Search(flag.Arg(0), flag.Arg(1))

	switch narg {
	case 2:
		// print all attributes
		for _, rec := range records {
			for _, tuple := range rec.Tuples {
				fmt.Printf("%s=%s ", tuple.Attr, tuple.Val)
			}
			fmt.Print("\n")
		}

	case 3:
		// only print rattr
		for _, rec := range records {
			for _, tuple := range rec.Tuples {
				if tuple.Attr == flag.Arg(2) {
					fmt.Printf("%s\n", tuple.Val)
				}
			}
		}
	}

}
