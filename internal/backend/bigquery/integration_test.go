package bigquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/arrowipc"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

// This test runs the backend against a real BigQuery project. It is the only
// thing in this repository that can: everything else runs against a fake of the
// REST API, which can say what the backend sends and how it reads an answer, not
// what BigQuery does.
//
// It runs only when SWH_TEST_BIGQUERY_PROJECT names a project it may use, and is
// skipped otherwise. It creates one dataset of its own, named with a random
// suffix, puts everything it makes inside it, and deletes it when it ends; it
// never reads or changes anything else in the project. The project needs the
// permissions to create datasets, tables and query jobs, and to stream rows.
//
//	SWH_TEST_BIGQUERY_PROJECT           the project to use (required to run)
//	SWH_TEST_BIGQUERY_LOCATION          where to create the dataset (optional; the project's default otherwise)
//	SWH_TEST_BIGQUERY_CREDENTIALS_FILE  a service-account key file (optional; Application Default Credentials otherwise)
const (
	envProject     = "SWH_TEST_BIGQUERY_PROJECT"
	envLocation    = "SWH_TEST_BIGQUERY_LOCATION"
	envCredentials = "SWH_TEST_BIGQUERY_CREDENTIALS_FILE"
)

func TestAgainstRealBigQuery(t *testing.T) {
	project := os.Getenv(envProject)
	if project == "" {
		t.Skipf("skipped: set %s to a BigQuery project this test may create a scratch dataset in "+
			"(optionally %s and %s); no BigQuery was contacted", envProject, envLocation, envCredentials)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	b, err := Open(ctx, backend.Config{
		Database:        project,
		Location:        os.Getenv(envLocation),
		CredentialsFile: os.Getenv(envCredentials),
	})
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	suffix := make([]byte, 6)
	_, err = rand.Read(suffix)
	require.NoError(t, err)
	dataset := "swh_it_" + hex.EncodeToString(suffix)
	t.Cleanup(func() {
		// The dataset is this test's own, so it is dropped with everything in it.
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := b.DropDataset(cleanup, dataset, backend.DropDatasetOptions{Cascade: true, IfExists: true}); err != nil {
			t.Errorf("could not delete the scratch dataset %s: %v", dataset, err)
		}
	})

	t.Run("catalog", func(t *testing.T) {
		created, err := b.CreateDataset(ctx, dataset, backend.CreateDatasetOptions{Description: "service-warehouse integration test"})
		require.NoError(t, err)
		require.Equal(t, dataset, created.Name)
		require.NotEmpty(t, created.Location)

		_, err = b.CreateDataset(ctx, dataset, backend.CreateDatasetOptions{})
		require.True(t, serr.Is(err, serr.AlreadyExists), "got %v", err)
		again, err := b.CreateDataset(ctx, dataset, backend.CreateDatasetOptions{IfNotExists: true})
		require.NoError(t, err)
		require.Equal(t, created.Name, again.Name)

		datasets, err := b.ListDatasets(ctx, backend.ListDatasetsOptions{Limit: 500})
		require.NoError(t, err)
		names := map[string]bool{}
		for _, d := range datasets.Datasets {
			names[d.Name] = true
		}
		if datasets.NextPageToken == "" {
			require.True(t, names[dataset], "the new dataset is listed")
		}

		ref := backend.TableRef{Dataset: dataset, Table: "events"}
		made, err := b.CreateTable(ctx, ref, backend.CreateTableOptions{
			Columns: []backend.Column{
				{Name: "id", Type: backend.TypeInt64},
				{Name: "name", Type: backend.TypeString, Nullable: true},
				{Name: "amount", Type: backend.TypeNumeric, Nullable: true, Precision: 10, Scale: 2},
				{Name: "at", Type: backend.TypeTimestampTZ, Nullable: true},
				{Name: "tags", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeString}}},
			},
			PartitionBy: []string{"at"},
			ClusterBy:   []string{"id"},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"at"}, made.PartitionBy)
		require.Equal(t, []string{"id"}, made.ClusterBy)
		require.Equal(t, backend.TypeInt64, made.Columns[0].Type)
		require.False(t, made.Columns[0].Nullable)
		require.Equal(t, "NUMERIC(10, 2)", made.Columns[2].NativeType)
		require.Equal(t, backend.TypeArray, made.Columns[4].Type)

		_, err = b.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{{Name: "id", Type: backend.TypeInt64}}})
		require.True(t, serr.Is(err, serr.AlreadyExists), "got %v", err)

		tables, err := b.ListTables(ctx, backend.ListTablesOptions{Dataset: dataset})
		require.NoError(t, err)
		require.Len(t, tables.Tables, 1)
		require.Equal(t, backend.KindTable, tables.Tables[0].Kind)

		err = b.DropDataset(ctx, dataset, backend.DropDatasetOptions{})
		require.True(t, serr.Is(err, serr.PreconditionFailed), "a dataset with a table needs cascade: %v", err)

		_, err = b.GetTable(ctx, backend.TableRef{Dataset: dataset, Table: "nope"})
		require.True(t, serr.Is(err, serr.NotFound), "got %v", err)
	})

	t.Run("insert and query", func(t *testing.T) {
		ref := backend.TableRef{Dataset: dataset, Table: "events"}
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "amount", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9}, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		}, nil)
		mem := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()
		// BigQuery streams only into recent partitions, so the rows are dated now.
		at := time.Now().UTC().Truncate(time.Hour)
		for i := int64(1); i <= 3; i++ {
			builder.Field(0).(*array.Int64Builder).Append(i)
			builder.Field(1).(*array.StringBuilder).Append("event " + string(rune('a'+i-1)))
			// Amounts are NUMERIC(38, 9): 10.00 and 20.00 fit the table's NUMERIC(10, 2); the
			// third, 99,999,999,999, has eleven digits before the point and does not.
			amount := decimal128.FromI64(i * 10_000_000_000)
			if i == 3 {
				amount = decimal128.FromBigInt(new(big.Int).Mul(big.NewInt(99_999_999_999), big.NewInt(1_000_000_000)))
			}
			builder.Field(2).(*array.Decimal128Builder).Append(amount)
			builder.Field(3).(*array.TimestampBuilder).Append(arrow.Timestamp(at.Add(time.Duration(i) * time.Hour).UnixMicro()))
			tags := builder.Field(4).(*array.ListBuilder)
			tags.Append(true)
			tags.ValueBuilder().(*array.StringBuilder).Append("t" + string(rune('0'+i)))
		}
		rec := builder.NewRecordBatch()
		defer rec.Release()
		header, err := arrowipc.SchemaMessage(schema)
		require.NoError(t, err)
		enc, err := arrowipc.NewEncoder(schema)
		require.NoError(t, err)
		defer enc.Close()
		batch, err := enc.Encode(rec)
		require.NoError(t, err)

		res, err := b.InsertRows(ctx, ref, header, &sliceReader{payloads: [][]byte{batch}})
		require.NoError(t, err)
		require.EqualValues(t, 2, res.RowsInserted)
		require.Len(t, res.Errors, 1)
		require.EqualValues(t, 2, res.Errors[0].RowIndex)
		require.Equal(t, backend.RefusalInvalidValue, res.Errors[0].Reason, "a value that does not fit its column is refused for that row alone")

		// A batch the table cannot take at all is refused as a schema mismatch.
		mismatch := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}, {Name: "no_such_column", Type: arrow.BinaryTypes.String}}, nil)
		mb := array.NewRecordBuilder(mem, mismatch)
		defer mb.Release()
		mb.Field(0).(*array.Int64Builder).Append(9)
		mb.Field(1).(*array.StringBuilder).Append("x")
		mrec := mb.NewRecordBatch()
		defer mrec.Release()
		mheader, err := arrowipc.SchemaMessage(mismatch)
		require.NoError(t, err)
		menc, err := arrowipc.NewEncoder(mismatch)
		require.NoError(t, err)
		defer menc.Close()
		mbatch, err := menc.Encode(mrec)
		require.NoError(t, err)
		res, err = b.InsertRows(ctx, ref, mheader, &sliceReader{payloads: [][]byte{mbatch}})
		require.NoError(t, err)
		require.Zero(t, res.RowsInserted)
		require.Len(t, res.Errors, 1)
		require.Equal(t, backend.RefusalSchemaMismatch, res.Errors[0].Reason)

		_, err = b.InsertRows(ctx, backend.TableRef{Dataset: dataset, Table: "nope"}, header, &sliceReader{payloads: [][]byte{batch}})
		require.True(t, serr.Is(err, serr.NotFound), "got %v", err)

		// Streamed rows become visible to queries within moments, not instantly.
		var result *backend.QueryResult
		require.Eventually(t, func() bool {
			r, err := b.Query(ctx, "SELECT id, name, amount, at, tags FROM `"+project+"."+dataset+".events` WHERE id >= @min ORDER BY id", backend.QueryOptions{
				Params: []backend.QueryParam{{Name: "min", Type: backend.TypeInt64, Value: "1"}},
			})
			if err != nil {
				t.Logf("query: %v", err)
				return false
			}
			if r.Job.Stats.RowsProduced != 2 {
				_ = r.Batches.Close()
				return false
			}
			result = r
			return true
		}, 2*time.Minute, 5*time.Second, "the two accepted rows are readable")
		defer result.Batches.Close()

		require.NotEmpty(t, result.Job.ID)
		require.Equal(t, backend.JobDone, result.Job.State)
		_, records := decode(t, result)
		require.Len(t, records, 1)
		require.EqualValues(t, 2, records[0].NumRows())
		require.Equal(t, int64(1), records[0].Column(0).(*array.Int64).Value(0))
		require.Equal(t, "event b", records[0].Column(1).(*array.String).Value(1))

		job, err := b.GetJob(ctx, result.Job.ID)
		require.NoError(t, err)
		require.Equal(t, backend.JobDone, job.State)
		require.Equal(t, result.Job.ID, job.ID)

		// Dry run prices without running.
		dry, err := b.Query(ctx, "SELECT id FROM `"+project+"."+dataset+".events`", backend.QueryOptions{DryRun: true})
		require.NoError(t, err)
		require.Nil(t, dry.Batches)
		require.Len(t, dry.Schema, 1)

		// A query over the byte cap is refused before it runs.
		_, err = b.Query(ctx, "SELECT id FROM `"+project+"."+dataset+".events`", backend.QueryOptions{MaxBytesBilled: 1})
		require.True(t, serr.Is(err, serr.Throttled), "got %v", err)

		_, err = b.Query(ctx, "SELEC 1", backend.QueryOptions{})
		require.True(t, serr.Is(err, serr.InvalidArgument), "got %v", err)
		_, err = b.Query(ctx, "SELECT * FROM `"+project+"."+dataset+".missing`", backend.QueryOptions{})
		require.True(t, serr.Is(err, serr.NotFound), "got %v", err)
	})

	t.Run("create table as select", func(t *testing.T) {
		ref := backend.TableRef{Dataset: dataset, Table: "numbers"}
		made, err := b.CreateTable(ctx, ref, backend.CreateTableOptions{AsSelect: "SELECT n, CAST(n AS STRING) AS label FROM UNNEST([1, 2, 3]) AS n"})
		require.NoError(t, err)
		require.Len(t, made.Columns, 2)
		require.EqualValues(t, 3, drain(t, b, "SELECT COUNT(*) FROM `"+project+"."+dataset+".numbers`"))

		require.NoError(t, b.DropTable(ctx, ref, backend.DropTableOptions{}))
		require.True(t, serr.Is(b.DropTable(ctx, ref, backend.DropTableOptions{}), serr.NotFound))
		require.NoError(t, b.DropTable(ctx, ref, backend.DropTableOptions{IfExists: true}))
	})
}

// drain runs a single-value query and returns its first cell as an integer.
func drain(t *testing.T, b *Backend, sql string) int64 {
	t.Helper()
	res, err := b.Query(context.Background(), sql, backend.QueryOptions{})
	require.NoError(t, err)
	defer res.Batches.Close()
	var value int64
	found := false
	for {
		raw, err := res.Batches.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		rec := readBack(t, res.ArrowSchema, raw)
		value, found = rec.Column(0).(*array.Int64).Value(0), true
	}
	require.True(t, found, "the query returned a row")
	return value
}
