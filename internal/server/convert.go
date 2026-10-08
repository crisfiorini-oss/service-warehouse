package server

import (
	"time"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/backend"
)

// This file is the single translation seam between the wire types (proto) and
// the Go-native backend types. Keeping it in one place means a backend never
// imports proto and the mapping is auditable in one read.

func unixMS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// --- ColumnType ---

var colTypeToProto = map[backend.ColumnType]whv0.ColumnType{
	backend.TypeUnknown:     whv0.ColumnType_COLUMN_TYPE_UNKNOWN,
	backend.TypeBool:        whv0.ColumnType_COLUMN_TYPE_BOOL,
	backend.TypeInt64:       whv0.ColumnType_COLUMN_TYPE_INT64,
	backend.TypeFloat64:     whv0.ColumnType_COLUMN_TYPE_FLOAT64,
	backend.TypeNumeric:     whv0.ColumnType_COLUMN_TYPE_NUMERIC,
	backend.TypeString:      whv0.ColumnType_COLUMN_TYPE_STRING,
	backend.TypeBytes:       whv0.ColumnType_COLUMN_TYPE_BYTES,
	backend.TypeDate:        whv0.ColumnType_COLUMN_TYPE_DATE,
	backend.TypeTime:        whv0.ColumnType_COLUMN_TYPE_TIME,
	backend.TypeTimestamp:   whv0.ColumnType_COLUMN_TYPE_TIMESTAMP,
	backend.TypeTimestampTZ: whv0.ColumnType_COLUMN_TYPE_TIMESTAMPTZ,
	backend.TypeInterval:    whv0.ColumnType_COLUMN_TYPE_INTERVAL,
	backend.TypeJSON:        whv0.ColumnType_COLUMN_TYPE_JSON,
	backend.TypeArray:       whv0.ColumnType_COLUMN_TYPE_ARRAY,
	backend.TypeStruct:      whv0.ColumnType_COLUMN_TYPE_STRUCT,
	backend.TypeGeography:   whv0.ColumnType_COLUMN_TYPE_GEOGRAPHY,
}

var protoToColType = func() map[whv0.ColumnType]backend.ColumnType {
	m := make(map[whv0.ColumnType]backend.ColumnType, len(colTypeToProto))
	for k, v := range colTypeToProto {
		m[v] = k
	}
	return m
}()

func colTypeOut(t backend.ColumnType) whv0.ColumnType {
	if p, ok := colTypeToProto[t]; ok {
		return p
	}
	return whv0.ColumnType_COLUMN_TYPE_UNKNOWN
}

func colTypeIn(p whv0.ColumnType) backend.ColumnType {
	if t, ok := protoToColType[p]; ok {
		return t
	}
	return backend.TypeUnknown
}

// --- Column ---

func columnOut(c backend.Column) *whv0.Column {
	pc := &whv0.Column{
		Name:        c.Name,
		Type:        colTypeOut(c.Type),
		Nullable:    c.Nullable,
		NativeType:  c.NativeType,
		Precision:   c.Precision,
		Scale:       c.Scale,
		Description: c.Description,
	}
	for _, f := range c.Fields {
		pc.Fields = append(pc.Fields, columnOut(f))
	}
	return pc
}

func columnsOut(cs []backend.Column) []*whv0.Column {
	if cs == nil {
		return nil
	}
	out := make([]*whv0.Column, 0, len(cs))
	for _, c := range cs {
		out = append(out, columnOut(c))
	}
	return out
}

func columnIn(pc *whv0.Column) backend.Column {
	c := backend.Column{
		Name:        pc.GetName(),
		Type:        colTypeIn(pc.GetType()),
		Nullable:    pc.GetNullable(),
		NativeType:  pc.GetNativeType(),
		Precision:   pc.GetPrecision(),
		Scale:       pc.GetScale(),
		Description: pc.GetDescription(),
	}
	for _, f := range pc.GetFields() {
		c.Fields = append(c.Fields, columnIn(f))
	}
	return c
}

func columnsIn(pcs []*whv0.Column) []backend.Column {
	if pcs == nil {
		return nil
	}
	out := make([]backend.Column, 0, len(pcs))
	for _, pc := range pcs {
		out = append(out, columnIn(pc))
	}
	return out
}

// --- TableRef ---

func tableRefIn(r *whv0.TableRef) backend.TableRef {
	return backend.TableRef{Dataset: r.GetDataset(), Table: r.GetTable()}
}

func tableRefOut(r backend.TableRef) *whv0.TableRef {
	return &whv0.TableRef{Dataset: r.Dataset, Table: r.Table}
}

// --- Job ---

var jobStateToProto = map[backend.JobState]whv0.JobState{
	backend.JobPending:   whv0.JobState_JOB_STATE_PENDING,
	backend.JobRunning:   whv0.JobState_JOB_STATE_RUNNING,
	backend.JobDone:      whv0.JobState_JOB_STATE_DONE,
	backend.JobFailed:    whv0.JobState_JOB_STATE_FAILED,
	backend.JobCancelled: whv0.JobState_JOB_STATE_CANCELLED,
}

func jobOut(j backend.Job) *whv0.Job {
	return &whv0.Job{
		Id:    j.ID,
		State: jobStateToProto[j.State],
		Stats: &whv0.JobStats{
			BytesScanned: j.Stats.BytesScanned,
			BytesBilled:  j.Stats.BytesBilled,
			RowsProduced: j.Stats.RowsProduced,
			RowsAffected: j.Stats.RowsAffected,
			SlotMillis:   j.Stats.SlotMillis,
			CacheHit:     j.Stats.CacheHit,
		},
		Error:         j.Error,
		CreatedUnixMs: unixMS(j.Created),
		StartedUnixMs: unixMS(j.Started),
		EndedUnixMs:   unixMS(j.Ended),
	}
}

// --- QueryParam / QueryOptions ---

func queryOptsIn(r *whv0.QueryRequest) backend.QueryOptions {
	opts := backend.QueryOptions{
		DefaultDataset: r.GetDefaultDataset(),
		DryRun:         r.GetDryRun(),
		MaxBytesBilled: r.GetMaxBytesBilled(),
		Timeout:        time.Duration(r.GetTimeoutMs()) * time.Millisecond,
	}
	for _, p := range r.GetParams() {
		opts.Params = append(opts.Params, backend.QueryParam{
			Name:   p.GetName(),
			Type:   colTypeIn(p.GetType()),
			Value:  p.GetValue(),
			IsNull: p.GetIsNull(),
		})
	}
	return opts
}

// --- TableInfo / TableSchema ---

var tableKindToProto = map[backend.TableKind]whv0.TableKind{
	backend.KindUnspecified:      whv0.TableKind_TABLE_KIND_UNSPECIFIED,
	backend.KindTable:            whv0.TableKind_TABLE_KIND_TABLE,
	backend.KindView:             whv0.TableKind_TABLE_KIND_VIEW,
	backend.KindMaterializedView: whv0.TableKind_TABLE_KIND_MATERIALIZED_VIEW,
	backend.KindExternal:         whv0.TableKind_TABLE_KIND_EXTERNAL,
}

func tableInfoOut(t backend.TableInfo) *whv0.TableInfo {
	return &whv0.TableInfo{
		Ref:            tableRefOut(t.Ref),
		Kind:           tableKindToProto[t.Kind],
		NumRows:        t.NumRows,
		NumBytes:       t.NumBytes,
		CreatedUnixMs:  unixMS(t.Created),
		ModifiedUnixMs: unixMS(t.Modified),
	}
}

func tableSchemaOut(s backend.TableSchema) *whv0.TableSchema {
	return &whv0.TableSchema{
		Info:        tableInfoOut(s.Info),
		Columns:     columnsOut(s.Columns),
		PartitionBy: s.PartitionBy,
		ClusterBy:   s.ClusterBy,
	}
}

// --- Dataset ---

func datasetOut(d backend.Dataset) *whv0.Dataset {
	return &whv0.Dataset{
		Name:        d.Name,
		Location:    d.Location,
		Description: d.Description,
		Labels:      d.Labels,
	}
}

// --- LoadFormat / WriteDisposition ---

var protoToLoadFormat = map[whv0.LoadFormat]backend.LoadFormat{
	whv0.LoadFormat_LOAD_FORMAT_UNSPECIFIED: backend.FormatUnspecified,
	whv0.LoadFormat_LOAD_FORMAT_CSV:         backend.FormatCSV,
	whv0.LoadFormat_LOAD_FORMAT_JSON:        backend.FormatJSON,
	whv0.LoadFormat_LOAD_FORMAT_PARQUET:     backend.FormatParquet,
	whv0.LoadFormat_LOAD_FORMAT_AVRO:        backend.FormatAvro,
	whv0.LoadFormat_LOAD_FORMAT_ORC:         backend.FormatORC,
}

var loadFormatToProto = func() map[backend.LoadFormat]whv0.LoadFormat {
	m := make(map[backend.LoadFormat]whv0.LoadFormat, len(protoToLoadFormat))
	for k, v := range protoToLoadFormat {
		m[v] = k
	}
	return m
}()

func loadFormatIn(p whv0.LoadFormat) backend.LoadFormat { return protoToLoadFormat[p] }

var protoToWriteDisp = map[whv0.WriteDisposition]backend.WriteDisposition{
	whv0.WriteDisposition_WRITE_DISPOSITION_UNSPECIFIED: backend.WriteAppend,
	whv0.WriteDisposition_WRITE_DISPOSITION_APPEND:      backend.WriteAppend,
	whv0.WriteDisposition_WRITE_DISPOSITION_TRUNCATE:    backend.WriteTruncate,
	whv0.WriteDisposition_WRITE_DISPOSITION_EMPTY:       backend.WriteEmpty,
}

// --- RowError ---

var rowRefusalToProto = map[backend.RowRefusal]whv0.RowRefusal{
	backend.RefusalUnspecified:    whv0.RowRefusal_ROW_REFUSAL_UNSPECIFIED,
	backend.RefusalInvalidValue:   whv0.RowRefusal_ROW_REFUSAL_INVALID_VALUE,
	backend.RefusalRowTooLarge:    whv0.RowRefusal_ROW_REFUSAL_ROW_TOO_LARGE,
	backend.RefusalSchemaMismatch: whv0.RowRefusal_ROW_REFUSAL_SCHEMA_MISMATCH,
}

// rowErrorOut maps a refusal a backend reported to the wire. A reason this
// server does not know leaves the refusal unspecified: the row is still
// refused, and nothing false is said about why.
func rowErrorOut(e backend.RowError) *whv0.RowError {
	return &whv0.RowError{
		RowIndex: e.RowIndex,
		Error:    e.Error,
		Reason:   rowRefusalToProto[e.Reason],
	}
}

// --- Capabilities ---

func capabilitiesOut(c backend.Capabilities) *whv0.BackendCapabilities {
	out := &whv0.BackendCapabilities{
		Backend:         c.Backend,
		AsyncJobs:       c.AsyncJobs,
		DryRun:          c.DryRun,
		Parameters:      c.Parameters,
		Ddl:             c.DDL,
		Load:            c.Load,
		Unload:          c.Unload,
		StreamingInsert: c.StreamingInsert,
		ArrowResults:    c.ArrowResults,
		MaxQueryBytes:   c.MaxQueryBytes,
		NativeVerbs:     c.NativeVerbs,
	}
	for _, f := range c.LoadFormats {
		out.LoadFormats = append(out.LoadFormats, loadFormatToProto[f])
	}
	return out
}
