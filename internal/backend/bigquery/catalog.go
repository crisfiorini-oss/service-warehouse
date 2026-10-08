package bigquery

import (
	"context"
	"fmt"

	bq "cloud.google.com/go/bigquery"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

const (
	// defaultPageSize is the page of a catalog listing that names no limit, and
	// maxPageSize the most a page holds: a larger request gets a full page and a
	// token to continue, which is how a client already reads every page.
	defaultPageSize = 50
	maxPageSize     = 500
	// metadataParallelism bounds the metadata reads one listing makes at once.
	// BigQuery lists only names, so each entry's detail is a request of its own,
	// which is also why a page is kept modest.
	metadataParallelism = 8
)

func pageSize(op string, requested int32) (int, error) {
	switch {
	case requested < 0:
		return 0, serr.New(serr.InvalidArgument, op, "the page limit must not be negative")
	case requested == 0:
		return defaultPageSize, nil
	case requested > maxPageSize:
		return maxPageSize, nil
	default:
		return int(requested), nil
	}
}

// describeEach reads the detail of every item of a page, a few at a time. An item
// that disappeared between the listing and the read (NotFound) is left out of the
// page; any other failure fails it.
func describeEach[In, Out any](ctx context.Context, op string, items []In, read func(context.Context, In) (Out, error)) ([]Out, error) {
	results := make([]Out, len(items))
	kept := make([]bool, len(items))
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(metadataParallelism)
	for i, item := range items {
		group.Go(func() error {
			out, err := read(ctx, item)
			if err != nil {
				if is(op, err, serr.NotFound) {
					return nil
				}
				return classify(op, err)
			}
			results[i], kept[i] = out, true
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	out := make([]Out, 0, len(items))
	for i := range items {
		if kept[i] {
			out = append(out, results[i])
		}
	}
	return out, nil
}

// ListDatasets lists the datasets of the project, a page at a time.
func (b *Backend) ListDatasets(ctx context.Context, opts backend.ListDatasetsOptions) (*backend.ListDatasetsResult, error) {
	const op = "ListDatasets"
	size, err := pageSize(op, opts.Limit)
	if err != nil {
		return nil, err
	}
	var page []*bq.Dataset
	next, err := iterator.NewPager(b.client.Datasets(ctx), size, opts.PageToken).NextPage(&page)
	if err != nil {
		return nil, classify(op, err)
	}
	datasets, err := describeEach(ctx, op, page, func(ctx context.Context, d *bq.Dataset) (backend.Dataset, error) {
		md, err := d.Metadata(ctx)
		if err != nil {
			return backend.Dataset{}, err
		}
		return datasetOut(d.DatasetID, md), nil
	})
	if err != nil {
		return nil, err
	}
	return &backend.ListDatasetsResult{Datasets: datasets, NextPageToken: next}, nil
}

func datasetOut(name string, md *bq.DatasetMetadata) backend.Dataset {
	return backend.Dataset{Name: name, Location: md.Location, Description: md.Description, Labels: md.Labels}
}

// ListTables lists the tables of a dataset, a page at a time.
func (b *Backend) ListTables(ctx context.Context, opts backend.ListTablesOptions) (*backend.ListTablesResult, error) {
	const op = "ListTables"
	dataset := opts.Dataset
	if dataset == "" {
		dataset = b.cfg.DefaultDataset
	}
	if dataset == "" {
		return nil, serr.New(serr.InvalidArgument, op, "no dataset is named and SWH_DATASET is not set")
	}
	if err := checkDatasetID(op, dataset); err != nil {
		return nil, err
	}
	size, err := pageSize(op, opts.Limit)
	if err != nil {
		return nil, err
	}
	var page []*bq.Table
	next, err := iterator.NewPager(b.client.Dataset(dataset).Tables(ctx), size, opts.PageToken).NextPage(&page)
	if err != nil {
		return nil, classify(op, err)
	}
	tables, err := describeEach(ctx, op, page, func(ctx context.Context, t *bq.Table) (backend.TableInfo, error) {
		md, err := t.Metadata(ctx)
		if err != nil {
			return backend.TableInfo{}, err
		}
		return tableInfoOut(backend.TableRef{Dataset: dataset, Table: t.TableID}, md), nil
	})
	if err != nil {
		return nil, err
	}
	return &backend.ListTablesResult{Tables: tables, NextPageToken: next}, nil
}

func tableKind(t bq.TableType) backend.TableKind {
	switch t {
	case bq.RegularTable:
		return backend.KindTable
	case bq.ViewTable:
		return backend.KindView
	case bq.MaterializedView:
		return backend.KindMaterializedView
	case bq.ExternalTable:
		return backend.KindExternal
	default:
		// A snapshot, or a kind added since: reported as what it is not known to
		// be, never as a table.
		return backend.KindUnspecified
	}
}

func tableInfoOut(ref backend.TableRef, md *bq.TableMetadata) backend.TableInfo {
	return backend.TableInfo{
		Ref:      ref,
		Kind:     tableKind(md.Type),
		NumRows:  int64(md.NumRows),
		NumBytes: md.NumBytes,
		Created:  md.CreationTime,
		Modified: md.LastModifiedTime,
	}
}

// partitionColumns names the partitioning column of a table. A table partitioned
// by ingestion time has none, and reports BigQuery's pseudo-column.
func partitionColumns(md *bq.TableMetadata) []string {
	switch {
	case md.TimePartitioning != nil && md.TimePartitioning.Field != "":
		return []string{md.TimePartitioning.Field}
	case md.TimePartitioning != nil:
		return []string{"_PARTITIONTIME"}
	case md.RangePartitioning != nil:
		return []string{md.RangePartitioning.Field}
	default:
		return nil
	}
}

func tableSchemaOut(ref backend.TableRef, md *bq.TableMetadata) backend.TableSchema {
	out := backend.TableSchema{
		Info:        tableInfoOut(ref, md),
		Columns:     columnsFromSchema(md.Schema),
		PartitionBy: partitionColumns(md),
	}
	if md.Clustering != nil {
		out.ClusterBy = md.Clustering.Fields
	}
	return out
}

// GetTable returns a table's columns, partitioning and clustering.
func (b *Backend) GetTable(ctx context.Context, ref backend.TableRef) (*backend.TableSchema, error) {
	const op = "GetTable"
	ref, err := b.resolve(op, ref)
	if err != nil {
		return nil, err
	}
	md, err := b.client.Dataset(ref.Dataset).Table(ref.Table).Metadata(ctx)
	if err != nil {
		return nil, classify(op, err)
	}
	out := tableSchemaOut(ref, md)
	return &out, nil
}

// CreateDataset creates a dataset, in the configured location when the request
// names none.
func (b *Backend) CreateDataset(ctx context.Context, name string, opts backend.CreateDatasetOptions) (*backend.Dataset, error) {
	const op = "CreateDataset"
	if err := checkDatasetID(op, name); err != nil {
		return nil, err
	}
	location := opts.Location
	if location == "" {
		location = b.cfg.Location
	}
	if location != "" && !locationID.MatchString(location) {
		return nil, serr.New(serr.InvalidArgument, op, fmt.Sprintf("%q is not a BigQuery location", location))
	}
	ds := b.client.Dataset(name)
	md := &bq.DatasetMetadata{Location: location, Description: opts.Description, Labels: opts.Labels}
	if err := ds.Create(ctx, md); err != nil {
		if !opts.IfNotExists || !is(op, err, serr.AlreadyExists) {
			return nil, classify(op, err)
		}
		existing, err := ds.Metadata(ctx)
		if err != nil {
			return nil, classify(op, err)
		}
		out := datasetOut(name, existing)
		return &out, nil
	}
	created, err := ds.Metadata(ctx)
	if err != nil {
		return nil, classify(op, err)
	}
	out := datasetOut(name, created)
	return &out, nil
}

// DropDataset deletes a dataset. Without cascade a dataset that still holds
// tables is not deleted.
func (b *Backend) DropDataset(ctx context.Context, name string, opts backend.DropDatasetOptions) error {
	const op = "DropDataset"
	if err := checkDatasetID(op, name); err != nil {
		return err
	}
	ds := b.client.Dataset(name)
	var err error
	if opts.Cascade {
		err = ds.DeleteWithContents(ctx)
	} else {
		err = ds.Delete(ctx)
	}
	if err != nil && opts.IfExists && is(op, err, serr.NotFound) {
		return nil
	}
	return classify(op, err)
}

// partitioning is the time partitioning a portable PartitionBy asks for. BigQuery
// partitions by one column, at day granularity here; a second column is not
// something it can do, so it is refused and not dropped.
func partitioning(op string, columns []string) (*bq.TimePartitioning, error) {
	switch len(columns) {
	case 0:
		return nil, nil
	case 1:
		field := columns[0]
		if field == "_PARTITIONTIME" || field == "_PARTITIONDATE" {
			return &bq.TimePartitioning{Type: bq.DayPartitioningType}, nil
		}
		return &bq.TimePartitioning{Type: bq.DayPartitioningType, Field: field}, nil
	default:
		return nil, serr.New(serr.Unsupported, op, "BigQuery partitions a table by one column")
	}
}

func clustering(columns []string) *bq.Clustering {
	if len(columns) == 0 {
		return nil
	}
	return &bq.Clustering{Fields: columns}
}

// CreateTable creates a table from columns, or from a query (CTAS).
func (b *Backend) CreateTable(ctx context.Context, ref backend.TableRef, opts backend.CreateTableOptions) (*backend.TableSchema, error) {
	const op = "CreateTable"
	ref, err := b.resolve(op, ref)
	if err != nil {
		return nil, err
	}
	if opts.AsSelect != "" && len(opts.Columns) > 0 {
		return nil, serr.New(serr.InvalidArgument, op, "a table is created from columns or from a query, not both")
	}
	if opts.AsSelect == "" && len(opts.Columns) == 0 {
		return nil, serr.New(serr.InvalidArgument, op, "a table needs columns or a query to be created from")
	}
	part, err := partitioning(op, opts.PartitionBy)
	if err != nil {
		return nil, err
	}
	table := b.client.Dataset(ref.Dataset).Table(ref.Table)

	if opts.IfNotExists {
		// Idempotent: an existing table is the answer, whatever it holds.
		if md, err := table.Metadata(ctx); err == nil {
			out := tableSchemaOut(ref, md)
			return &out, nil
		} else if !is(op, err, serr.NotFound) {
			return nil, classify(op, err)
		}
	}

	if opts.AsSelect != "" {
		if err := b.createAsSelect(ctx, table, opts, part); err != nil {
			return nil, err
		}
	} else {
		schema, err := schemaFromColumns(opts.Columns)
		if err != nil {
			return nil, err
		}
		meta := &bq.TableMetadata{Schema: schema, TimePartitioning: part, Clustering: clustering(opts.ClusterBy)}
		if err := table.Create(ctx, meta); err != nil {
			return nil, classify(op, err)
		}
	}
	return b.GetTable(ctx, ref)
}

// createAsSelect runs the query into the new table, which fails if the table
// already holds rows.
func (b *Backend) createAsSelect(ctx context.Context, table *bq.Table, opts backend.CreateTableOptions, part *bq.TimePartitioning) error {
	const op = "CreateTable"
	timeout := b.cfg.QueryTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	q := b.client.Query(opts.AsSelect)
	q.Dst = table
	q.CreateDisposition = bq.CreateIfNeeded
	q.WriteDisposition = bq.WriteEmpty
	q.TimePartitioning = part
	q.Clustering = clustering(opts.ClusterBy)
	q.MaxBytesBilled = b.cfg.MaxQueryBytes
	if b.cfg.DefaultDataset != "" {
		q.DefaultProjectID, q.DefaultDatasetID = b.project, b.cfg.DefaultDataset
	}
	if timeout > 0 {
		q.JobTimeout = timeout
	}
	job, err := q.Run(ctx)
	if err != nil {
		return classify(op, err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		cancelAbandoned(ctx, job)
		return classify(op, err)
	}
	return classify(op, status.Err())
}

// DropTable deletes a table.
func (b *Backend) DropTable(ctx context.Context, ref backend.TableRef, opts backend.DropTableOptions) error {
	const op = "DropTable"
	ref, err := b.resolve(op, ref)
	if err != nil {
		return err
	}
	err = b.client.Dataset(ref.Dataset).Table(ref.Table).Delete(ctx)
	if err != nil && opts.IfExists && is(op, err, serr.NotFound) {
		return nil
	}
	return classify(op, err)
}
