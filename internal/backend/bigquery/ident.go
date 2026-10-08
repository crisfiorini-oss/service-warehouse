package bigquery

import (
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Names a caller supplies become parts of a request path, and the client library
// expands them without escaping reserved characters. A dataset named
// "d/tables/t" would then address a table, so every name is checked against what
// BigQuery itself allows before it is used: nothing a name contains can change
// what is addressed.

// identifier is a name pattern with the longest name BigQuery accepts for it.
type identifier struct {
	re  *regexp.Regexp
	max int
}

func (i identifier) MatchString(s string) bool {
	return s != "" && utf8.RuneCountInString(s) <= i.max && i.re.MatchString(s)
}

var (
	// datasetID: letters, digits and underscores.
	datasetID = identifier{regexp.MustCompile(`^[A-Za-z0-9_]+$`), 1024}
	// tableID: Unicode letters, marks, numbers, connectors, dashes and spaces.
	tableID = identifier{regexp.MustCompile(`^[\p{L}\p{M}\p{N}\p{Pc}\p{Pd} ]+$`), 1024}
	// jobID: letters, digits, underscores and dashes.
	jobID = identifier{regexp.MustCompile(`^[A-Za-z0-9_-]+$`), 1024}
	// locationID: a region or multi-region such as US, EU or europe-west1.
	locationID = identifier{regexp.MustCompile(`^[A-Za-z0-9-]+$`), 64}
)

func checkDatasetID(op, name string) error {
	if !datasetID.MatchString(name) {
		return serr.New(serr.InvalidArgument, op,
			fmt.Sprintf("%q is not a valid dataset name (letters, digits and underscores)", name))
	}
	return nil
}

func checkTableID(op, name string) error {
	if !tableID.MatchString(name) {
		return serr.New(serr.InvalidArgument, op,
			fmt.Sprintf("%q is not a valid table name (letters, digits, underscores, dashes and spaces)", name))
	}
	return nil
}
