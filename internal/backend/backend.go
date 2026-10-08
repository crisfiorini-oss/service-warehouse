// Package backend defines the provider-agnostic contract every warehouse
// backend implements. The types are Go-native (no proto dependency) so backends
// stay testable and the gRPC layer maps to/from them. The interface is the
// honest intersection across BigQuery, Snowflake, Redshift, ClickHouse, and
// DuckDB; anything backend-specific is reached only through Native.
package backend

import (
	"context"
	"io"
	"time"
)

// Config is the resolved configuration for opening one backend against one
// database (a BigQuery project, a Snowflake / Redshift database, a ClickHouse
// server, or a DuckDB file). A backend uses the subset it needs.
type Config struct {
	// Kind selects the implementation:
	// "duckdb" | "bigquery" | "snowflake" | "redshift" | "clickhouse".
	Kind string
	// Database is the top-level container the server is bound to (BigQuery
	// project, Snowflake/Redshift database, ClickHouse — or a DuckDB path).
	Database string
	// DefaultDataset resolves unqualified table names (BigQuery dataset,
	// Snowflake/Redshift schema, ClickHouse database).
	DefaultDataset string
	// Location is the geographic region for datasets/jobs where it matters (BQ).
	Location string

	// DSN, when set, is a backend-native connection string that overrides the
	// discrete fields below (Snowflake/Redshift/ClickHouse).
	DSN string

	// Host / Port / User / Password / Account authenticate the SQL backends.
	Host     string
	Port     int
	User     string
	Password string
	Account  string // Snowflake account identifier

	// CredentialsFile is a service-account JSON path for BigQuery (empty = ADC).
	CredentialsFile string

	// MaxQueryBytes caps a single query's scan; 0 means the backend default.
	MaxQueryBytes int64
	// QueryTimeout bounds a single query; 0 means the backend default.
	QueryTimeout time.Duration
}

// ColumnType is the portable logical type — the honest intersection across the
// SQL warehouses. Anything a backend cannot map is TypeUnknown with the exact
// backend spelling preserved in Column.NativeType.
type ColumnType int

const (
	TypeUnknown ColumnType = iota
	TypeBool
	TypeInt64
	TypeFloat64
	TypeNumeric
	TypeString
	TypeBytes
	TypeDate
	TypeTime
	TypeTimestamp
	TypeTimestampTZ
	TypeInterval
	TypeJSON
	TypeArray
	TypeStruct
	TypeGeography
)

// Column describes one field of a table or result set. NativeType is the exact
// backend spelling; Type is the portable bucket.
type Column struct {
	Name        string
	Type        ColumnType
	Nullable    bool
	NativeType  string
	Precision   int32
	Scale       int32
	Description string
	// Fields is the element type (ARRAY) or the sub-fields (STRUCT).
	Fields []Column
}

// TableRef names a table within the bound database. Dataset is the namespace
// (BigQuery dataset / Snowflake+Redshift schema / ClickHouse database).
type TableRef struct {
	Dataset string
	Table   string
}

// JobState is the lifecycle of an asynchronous unit of work.
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobDone
	JobFailed
	JobCancelled
)

// JobStats is best-effort accounting; a field is 0 when unreported.
type JobStats struct {
	BytesScanned int64
	BytesBilled  int64
	RowsProduced int64
	RowsAffected int64
	SlotMillis   int64
	CacheHit     bool
}

// Job is the handle for an asynchronous operation.
type Job struct {
	ID      string
	State   JobState
	Stats   JobStats
	Error   string
	Created time.Time
	Started time.Time
	Ended   time.Time
}

// QueryParam is a named bind parameter. Value is the scalar rendered as text;
// the backend binds it type-safely.
type QueryParam struct {
	Name   string
	Type   ColumnType
	Value  string
	IsNull bool
}

// QueryOptions carries everything a Query needs besides the SQL text.
type QueryOptions struct {
	Params         []QueryParam
	DefaultDataset string
	DryRun         bool
	MaxBytesBilled int64
	Timeout        time.Duration
}

// BatchReader yields Arrow-IPC record batches. Next returns io.EOF when the
// result is exhausted. Close releases the underlying cursor.
type BatchReader interface {
	// Next returns the next Arrow-IPC record batch, or io.EOF at the end.
	Next() ([]byte, error)
	io.Closer
}

// QueryResult is a streaming query outcome. Batches is nil for a dry run or a
// DDL/DML statement (Schema is then empty too). ArrowSchema is the Arrow-IPC
// schema message matching Batches, so a client feeds batches straight to an
// Arrow reader.
type QueryResult struct {
	Job         Job
	Schema      []Column
	ArrowSchema []byte
	Batches     BatchReader
}

// TableKind distinguishes a physical table from a view / external table.
type TableKind int

const (
	KindUnspecified TableKind = iota
	KindTable
	KindView
	KindMaterializedView
	KindExternal
)

// TableInfo is a table's identity and coarse size.
type TableInfo struct {
	Ref      TableRef
	Kind     TableKind
	NumRows  int64
	NumBytes int64
	Created  time.Time
	Modified time.Time
}

// TableSchema is a table's full description.
type TableSchema struct {
	Info        TableInfo
	Columns     []Column
	PartitionBy []string
	ClusterBy   []string
}

// Dataset is a namespace within the bound database.
type Dataset struct {
	Name        string
	Location    string
	Description string
	Labels      map[string]string
}

// ListDatasetsOptions / ListTablesOptions page catalog listings with an opaque
// token.
type ListDatasetsOptions struct {
	PageToken string
	Limit     int32
}

type ListTablesOptions struct {
	Dataset   string
	PageToken string
	Limit     int32
}

// ListDatasetsResult / ListTablesResult carry a page plus the next token; an
// empty NextPageToken ends iteration.
type ListDatasetsResult struct {
	Datasets      []Dataset
	NextPageToken string
}

type ListTablesResult struct {
	Tables        []TableInfo
	NextPageToken string
}

// CreateDatasetOptions / CreateTableOptions / DropOptions carry the idempotency
// and cascade intent of DDL.
type CreateDatasetOptions struct {
	Location    string
	Description string
	Labels      map[string]string
	IfNotExists bool
}

type CreateTableOptions struct {
	Columns     []Column
	PartitionBy []string
	ClusterBy   []string
	IfNotExists bool
	// AsSelect creates the table from a query (CTAS); Columns may be empty.
	AsSelect string
}

type DropDatasetOptions struct {
	Cascade  bool
	IfExists bool
}

type DropTableOptions struct {
	IfExists bool
}

// LoadFormat is the on-disk format of the files a Load reads.
type LoadFormat int

const (
	FormatUnspecified LoadFormat = iota
	FormatCSV
	FormatJSON
	FormatParquet
	FormatAvro
	FormatORC
)

// WriteDisposition controls how a Load/CTAS resolves against an existing table.
type WriteDisposition int

const (
	WriteAppend WriteDisposition = iota
	WriteTruncate
	WriteEmpty
)

// LoadOptions describes a bulk load from object-storage URIs into a table. The
// backend reads the URIs directly; bytes never transit the gateway.
type LoadOptions struct {
	Dest             TableRef
	SourceURIs       []string
	Format           LoadFormat
	WriteDisposition WriteDisposition
	Schema           []Column
	Options          map[string]string
}

// UnloadOptions exports a table or a query to an object-storage prefix. Exactly
// one of Table / SQL is set.
type UnloadOptions struct {
	Table   *TableRef
	SQL     string
	DestURI string
	Format  LoadFormat
	Options map[string]string
}

// RowRefusal is the portable reason a backend permanently refused one row of a
// streaming insert. It tells a caller what to do next: retrying the same row
// against the same table gets the same answer, unless the reason says the
// table, not the row, is what disagrees. A failure that says nothing against the
// row (a throttle, a quota, a timeout, a dead backend) is never a row refusal;
// the insert fails with a serr code instead.
type RowRefusal int

const (
	// RefusalUnspecified is a refusal the backend does not explain, or one no
	// other reason fits. It is still a refusal of the row.
	RefusalUnspecified RowRefusal = iota
	// RefusalInvalidValue means a value cannot be stored in its column: the
	// wrong type, out of range, malformed, or null in a required column.
	RefusalInvalidValue
	// RefusalRowTooLarge means the row alone exceeds what the backend accepts.
	RefusalRowTooLarge
	// RefusalSchemaMismatch means the row's columns disagree with the table's:
	// a column the table lacks, or a required column the row does not carry. The
	// table would have to change for the same row to be accepted.
	RefusalSchemaMismatch
)

// RowError is a per-row rejection from a streaming insert. RowIndex counts rows
// from 0 across every batch of the call, in the order they were read. Error is
// a description the backend wrote itself, never a vendor's own text; callers
// branch on Reason.
type RowError struct {
	RowIndex int64
	Error    string
	Reason   RowRefusal
}

// InsertResult reports a streaming insert outcome.
type InsertResult struct {
	RowsInserted int64
	Errors       []RowError
}

// Capabilities is the machine-readable feature set a client introspects before
// calling — a false flag means "call it and get Unsupported".
type Capabilities struct {
	Backend         string
	AsyncJobs       bool
	DryRun          bool
	Parameters      bool
	DDL             bool
	Load            bool
	Unload          bool
	StreamingInsert bool
	ArrowResults    bool
	MaxQueryBytes   int64
	LoadFormats     []LoadFormat
	NativeVerbs     []string
}

// Backend is the provider-agnostic warehouse contract. Implementations
// translate to exactly one engine and return serr.Error-coded failures.
type Backend interface {
	// Name is the backend kind ("duckdb", "bigquery", …).
	Name() string
	// Capabilities reports what this backend honors.
	Capabilities() Capabilities

	// Query executes SQL and streams Arrow result batches. A dry run returns a
	// priced Job and schema with a nil BatchReader.
	Query(ctx context.Context, sql string, opts QueryOptions) (*QueryResult, error)
	// GetJob reports the status of a previously started job.
	GetJob(ctx context.Context, id string) (*Job, error)
	// CancelJob requests cancellation of a running job.
	CancelJob(ctx context.Context, id string) error

	// ListDatasets enumerates namespaces in the bound database.
	ListDatasets(ctx context.Context, opts ListDatasetsOptions) (*ListDatasetsResult, error)
	// ListTables enumerates tables in a dataset.
	ListTables(ctx context.Context, opts ListTablesOptions) (*ListTablesResult, error)
	// GetTable returns a table's full schema.
	GetTable(ctx context.Context, ref TableRef) (*TableSchema, error)
	// CreateDataset / DropDataset manage namespaces.
	CreateDataset(ctx context.Context, name string, opts CreateDatasetOptions) (*Dataset, error)
	DropDataset(ctx context.Context, name string, opts DropDatasetOptions) error
	// CreateTable / DropTable manage tables.
	CreateTable(ctx context.Context, ref TableRef, opts CreateTableOptions) (*TableSchema, error)
	DropTable(ctx context.Context, ref TableRef, opts DropTableOptions) error

	// Load bulk-loads object-storage files into a table, returning a Job.
	Load(ctx context.Context, opts LoadOptions) (*Job, error)
	// Unload exports a table or query to object storage, returning a Job.
	Unload(ctx context.Context, opts UnloadOptions) (*Job, error)
	// InsertRows appends Arrow batches to a table. schema is the Arrow-IPC
	// schema of the batches (may be nil when self-describing).
	InsertRows(ctx context.Context, ref TableRef, schema []byte, batches BatchReader) (*InsertResult, error)

	// Native is the escape hatch for backend-specific verbs.
	Native(ctx context.Context, verb string, params map[string]string) (map[string]string, error)
	// Close releases resources.
	Close() error
}
