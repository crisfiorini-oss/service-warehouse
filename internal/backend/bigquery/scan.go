package bigquery

import (
	"context"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// Scan is not offered yet: reading a table without a query job is a Storage Read
// API session, which this backend does not open, and a gap is Unsupported.
func (b *Backend) Scan(context.Context, backend.TableRef, backend.ScanOptions) (*backend.QueryResult, error) {
	return nil, serr.New(serr.Unsupported, "Scan", "the bigquery backend does not offer table scans yet")
}
