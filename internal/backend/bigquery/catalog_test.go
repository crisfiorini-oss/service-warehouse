package bigquery

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/serr"
)

const (
	datasetsPath = "/projects/test-project/datasets"
	tablesPath   = "/projects/test-project/datasets/d/tables"
	tablePath    = "/projects/test-project/datasets/d/tables/t"
)

func datasetRef(id string) map[string]any {
	return map[string]any{"projectId": "test-project", "datasetId": id}
}

func tableRef(dataset, id string) map[string]any {
	return map[string]any{"projectId": "test-project", "datasetId": dataset, "tableId": id}
}

func TestListDatasetsReadsEachDatasetsDetailAndPages(t *testing.T) {
	f := newFake(t)
	f.handle("GET "+datasetsPath, 200, map[string]any{
		"datasets":      []map[string]any{{"datasetReference": datasetRef("a")}, {"datasetReference": datasetRef("gone")}, {"datasetReference": datasetRef("b")}},
		"nextPageToken": "next-page",
	})
	f.handle("GET "+datasetsPath+"/a", 200, map[string]any{
		"datasetReference": datasetRef("a"), "location": "EU", "description": "analytics", "labels": map[string]string{"team": "x"},
	})
	f.handle("GET "+datasetsPath+"/b", 200, map[string]any{"datasetReference": datasetRef("b"), "location": "US"})
	f.handle("GET "+datasetsPath+"/gone", 404, apiError(404, "notFound", "Not found"))
	b := f.open(t, backend.Config{})

	res, err := b.ListDatasets(context.Background(), backend.ListDatasetsOptions{PageToken: "this-page", Limit: 3})
	require.NoError(t, err)
	require.Equal(t, []backend.Dataset{
		{Name: "a", Location: "EU", Description: "analytics", Labels: map[string]string{"team": "x"}},
		{Name: "b", Location: "US"},
	}, res.Datasets, "a dataset deleted between the listing and the read is left out")
	require.Equal(t, "next-page", res.NextPageToken)

	listed := f.seen("GET", datasetsPath)[0]
	require.Equal(t, "this-page", listed.Query["pageToken"][0])
	require.Equal(t, "3", listed.Query["maxResults"][0])
}

func TestAPageIsNeverLargerThanTheMaximumAndNeverNegative(t *testing.T) {
	f := newFake(t)
	f.handle("GET "+datasetsPath, 200, map[string]any{})
	b := f.open(t, backend.Config{})

	_, err := b.ListDatasets(context.Background(), backend.ListDatasetsOptions{Limit: 1 << 20})
	require.NoError(t, err)
	require.Equal(t, "500", f.seen("GET", datasetsPath)[0].Query["maxResults"][0])

	_, err = b.ListDatasets(context.Background(), backend.ListDatasetsOptions{})
	require.NoError(t, err)
	require.Equal(t, "50", f.seen("GET", datasetsPath)[1].Query["maxResults"][0], "no limit asks for the default page")

	_, err = b.ListDatasets(context.Background(), backend.ListDatasetsOptions{Limit: -1})
	require.True(t, serr.Is(err, serr.InvalidArgument))
}

func TestListTablesReportsKindAndSize(t *testing.T) {
	f := newFake(t)
	f.handle("GET "+tablesPath, 200, map[string]any{"tables": []map[string]any{
		{"tableReference": tableRef("d", "events"), "type": "TABLE"},
		{"tableReference": tableRef("d", "v"), "type": "VIEW"},
		{"tableReference": tableRef("d", "snap"), "type": "SNAPSHOT"},
	}})
	f.handle("GET "+tablesPath+"/events", 200, map[string]any{
		"tableReference": tableRef("d", "events"), "type": "TABLE", "numRows": "42", "numBytes": "4200",
		"creationTime": "1700000000000", "lastModifiedTime": "1700000005000",
	})
	f.handle("GET "+tablesPath+"/v", 200, map[string]any{"tableReference": tableRef("d", "v"), "type": "VIEW"})
	f.handle("GET "+tablesPath+"/snap", 200, map[string]any{"tableReference": tableRef("d", "snap"), "type": "SNAPSHOT"})
	b := f.open(t, backend.Config{})

	res, err := b.ListTables(context.Background(), backend.ListTablesOptions{Dataset: "d"})
	require.NoError(t, err)
	require.Len(t, res.Tables, 3)
	require.Equal(t, backend.TableRef{Dataset: "d", Table: "events"}, res.Tables[0].Ref)
	require.Equal(t, backend.KindTable, res.Tables[0].Kind)
	require.EqualValues(t, 42, res.Tables[0].NumRows)
	require.EqualValues(t, 4200, res.Tables[0].NumBytes)
	require.EqualValues(t, 1700000000000, res.Tables[0].Created.UnixMilli())
	require.Equal(t, backend.KindView, res.Tables[1].Kind)
	require.Equal(t, backend.KindUnspecified, res.Tables[2].Kind, "a snapshot is not reported as a table")
	require.Empty(t, res.NextPageToken)

	_, err = b.ListTables(context.Background(), backend.ListTablesOptions{})
	require.True(t, serr.Is(err, serr.InvalidArgument), "no dataset is named and none is configured")
}

func TestGetTableDescribesColumnsPartitioningAndClustering(t *testing.T) {
	f := newFake(t)
	f.handle("GET "+tablePath, 200, map[string]any{
		"tableReference": tableRef("d", "t"), "type": "TABLE", "numRows": "7",
		"schema": map[string]any{"fields": []map[string]any{
			{"name": "id", "type": "INTEGER", "mode": "REQUIRED", "description": "the key"},
			{"name": "amount", "type": "NUMERIC", "mode": "NULLABLE", "precision": "10", "scale": "2"},
			{"name": "tags", "type": "STRING", "mode": "REPEATED"},
			{"name": "at", "type": "TIMESTAMP", "mode": "NULLABLE"},
			{"name": "items", "type": "RECORD", "mode": "REPEATED", "fields": []map[string]any{
				{"name": "sku", "type": "STRING", "mode": "NULLABLE", "maxLength": "12"},
				{"name": "qty", "type": "INTEGER", "mode": "NULLABLE"},
			}},
			{"name": "span", "type": "RANGE", "mode": "NULLABLE", "rangeElementType": map[string]any{"type": "DATE"}},
		}},
		"timePartitioning": map[string]any{"type": "DAY", "field": "at"},
		"clustering":       map[string]any{"fields": []string{"id"}},
	})
	b := f.open(t, backend.Config{})

	got, err := b.GetTable(context.Background(), backend.TableRef{Dataset: "d", Table: "t"})
	require.NoError(t, err)
	require.Equal(t, []string{"at"}, got.PartitionBy)
	require.Equal(t, []string{"id"}, got.ClusterBy)
	require.EqualValues(t, 7, got.Info.NumRows)
	require.Len(t, got.Columns, 6)

	require.Equal(t, backend.Column{Name: "id", Type: backend.TypeInt64, NativeType: "INT64", Description: "the key"}, got.Columns[0])
	require.Equal(t, backend.Column{Name: "amount", Type: backend.TypeNumeric, Nullable: true, NativeType: "NUMERIC(10, 2)", Precision: 10, Scale: 2}, got.Columns[1])
	require.Equal(t, backend.Column{
		Name: "tags", Type: backend.TypeArray, NativeType: "ARRAY<STRING>",
		Fields: []backend.Column{{Name: "item", Type: backend.TypeString, NativeType: "STRING"}},
	}, got.Columns[2])
	require.Equal(t, backend.TypeTimestampTZ, got.Columns[3].Type)
	items := got.Columns[4]
	require.Equal(t, backend.TypeArray, items.Type)
	require.Equal(t, "ARRAY<STRUCT<sku STRING(12), qty INT64>>", items.NativeType)
	require.Equal(t, backend.TypeStruct, items.Fields[0].Type)
	require.Equal(t, []string{"sku", "qty"}, []string{items.Fields[0].Fields[0].Name, items.Fields[0].Fields[1].Name})
	require.Equal(t, backend.Column{Name: "span", Type: backend.TypeUnknown, Nullable: true, NativeType: "RANGE<DATE>"}, got.Columns[5],
		"a type with no portable bucket is UNKNOWN with BigQuery's spelling kept")
}

func TestGetTableReportsIngestionTimePartitioningByItsPseudoColumn(t *testing.T) {
	f := newFake(t)
	f.handle("GET "+tablePath, 200, map[string]any{"tableReference": tableRef("d", "t"), "type": "TABLE", "timePartitioning": map[string]any{"type": "DAY"}})
	b := f.open(t, backend.Config{})
	got, err := b.GetTable(context.Background(), backend.TableRef{Dataset: "d", Table: "t"})
	require.NoError(t, err)
	require.Equal(t, []string{"_PARTITIONTIME"}, got.PartitionBy)
}

func TestCreateDatasetUsesTheConfiguredLocationAndIsIdempotentOnRequest(t *testing.T) {
	quiet(t)
	f := newFake(t)
	created := map[string]any{"datasetReference": datasetRef("fresh"), "location": "EU", "description": "x"}
	f.handle("POST "+datasetsPath, 200, created)
	f.handle("GET "+datasetsPath+"/fresh", 200, created)
	b := f.open(t, backend.Config{Location: "EU"})

	got, err := b.CreateDataset(context.Background(), "fresh", backend.CreateDatasetOptions{Description: "x", Labels: map[string]string{"k": "v"}})
	require.NoError(t, err)
	require.Equal(t, backend.Dataset{Name: "fresh", Location: "EU", Description: "x"}, *got)
	body := f.seen("POST", datasetsPath)[0].Body
	require.Equal(t, "EU", body["location"])
	require.Equal(t, map[string]any{"k": "v"}, body["labels"])
	require.Equal(t, "fresh", body["datasetReference"].(map[string]any)["datasetId"])

	exists := newFake(t)
	exists.handle("POST "+datasetsPath, 409, apiError(409, "duplicate", "Already Exists: Dataset test-project:fresh"))
	exists.handle("GET "+datasetsPath+"/fresh", 200, created)
	b2 := exists.open(t, backend.Config{})
	_, err = b2.CreateDataset(context.Background(), "fresh", backend.CreateDatasetOptions{})
	require.True(t, serr.Is(err, serr.AlreadyExists), "got %v", err)
	again, err := b2.CreateDataset(context.Background(), "fresh", backend.CreateDatasetOptions{IfNotExists: true})
	require.NoError(t, err)
	require.Equal(t, "fresh", again.Name)

	_, err = b.CreateDataset(context.Background(), "x", backend.CreateDatasetOptions{Location: "not a/location"})
	require.True(t, serr.Is(err, serr.InvalidArgument))
}

func TestDropDatasetCascadesOnlyWhenAskedAndMapsWhatBigQuerySays(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handleFunc("DELETE "+datasetsPath+"/full", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("deleteContents") != "true" {
			writeJSON(w, 400, apiError(400, "resourceInUse", "Dataset test-project:full is still in use"))
			return
		}
		w.WriteHeader(204)
	})
	f.handle("DELETE "+datasetsPath+"/missing", 404, apiError(404, "notFound", "Not found: Dataset"))
	b := f.open(t, backend.Config{})

	err := b.DropDataset(context.Background(), "full", backend.DropDatasetOptions{})
	require.True(t, serr.Is(err, serr.PreconditionFailed), "a dataset that still holds tables needs cascade: %v", err)
	require.NoError(t, b.DropDataset(context.Background(), "full", backend.DropDatasetOptions{Cascade: true}))

	require.True(t, serr.Is(b.DropDataset(context.Background(), "missing", backend.DropDatasetOptions{}), serr.NotFound))
	require.NoError(t, b.DropDataset(context.Background(), "missing", backend.DropDatasetOptions{IfExists: true}))
	require.Len(t, f.seen("DELETE", datasetsPath+"/full"), 2, "the cascade is the only second attempt, and it is the caller's")
}

func TestCreateTableSendsColumnsPartitioningAndClusteringThenReadsTheTableBack(t *testing.T) {
	f := newFake(t)
	f.handle("POST "+tablesPath, 200, map[string]any{"tableReference": tableRef("d", "t")})
	f.handle("GET "+tablePath, 200, map[string]any{
		"tableReference": tableRef("d", "t"), "type": "TABLE",
		"schema": map[string]any{"fields": []map[string]any{field("id", "INTEGER", "REQUIRED")}},
	})
	b := f.open(t, backend.Config{})

	got, err := b.CreateTable(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, backend.CreateTableOptions{
		Columns: []backend.Column{
			{Name: "id", Type: backend.TypeInt64},
			{Name: "at", Type: backend.TypeTimestampTZ, Nullable: true},
			{Name: "amount", Type: backend.TypeNumeric, Nullable: true, Precision: 10, Scale: 2},
			{Name: "big", Type: backend.TypeNumeric, Nullable: true, Precision: 50, Scale: 20},
			{Name: "tags", Type: backend.TypeArray, Fields: []backend.Column{{Name: "item", Type: backend.TypeString}}},
			{Name: "owner", Type: backend.TypeStruct, Nullable: true, Fields: []backend.Column{
				{Name: "name", Type: backend.TypeString, Nullable: true},
			}},
		},
		PartitionBy: []string{"at"},
		ClusterBy:   []string{"id"},
	})
	require.NoError(t, err)
	require.Equal(t, "id", got.Columns[0].Name, "the answer is the table as BigQuery now holds it")

	body := f.seen("POST", tablesPath)[0].Body
	fields := body["schema"].(map[string]any)["fields"].([]any)
	require.Len(t, fields, 6)
	byName := map[string]map[string]any{}
	for _, f := range fields {
		byName[f.(map[string]any)["name"].(string)] = f.(map[string]any)
	}
	require.Equal(t, "INTEGER", byName["id"]["type"])
	require.Equal(t, "REQUIRED", byName["id"]["mode"])
	require.Equal(t, "TIMESTAMP", byName["at"]["type"])
	require.Equal(t, "NUMERIC", byName["amount"]["type"])
	require.Equal(t, "10", byName["amount"]["precision"])
	require.Equal(t, "BIGNUMERIC", byName["big"]["type"], "a precision NUMERIC cannot hold is BIGNUMERIC")
	require.Equal(t, "REPEATED", byName["tags"]["mode"])
	require.Equal(t, "STRING", byName["tags"]["type"])
	require.Equal(t, "RECORD", byName["owner"]["type"])
	require.Equal(t, map[string]any{"type": "DAY", "field": "at"}, body["timePartitioning"])
	require.Equal(t, map[string]any{"fields": []any{"id"}}, body["clustering"])
}

func TestCreateTableIsIdempotentOnRequestAndRefusesWhatItCannotMake(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("GET "+tablePath, 200, map[string]any{"tableReference": tableRef("d", "t"), "type": "TABLE",
		"schema": map[string]any{"fields": []map[string]any{field("id", "INTEGER", "NULLABLE")}}})
	f.handle("POST "+tablesPath, 409, apiError(409, "duplicate", "Already Exists: Table"))
	b := f.open(t, backend.Config{})
	ref := backend.TableRef{Dataset: "d", Table: "t"}
	cols := []backend.Column{{Name: "id", Type: backend.TypeInt64, Nullable: true}}

	got, err := b.CreateTable(context.Background(), ref, backend.CreateTableOptions{Columns: cols, IfNotExists: true})
	require.NoError(t, err)
	require.Equal(t, "id", got.Columns[0].Name)
	require.Empty(t, f.seen("POST", tablesPath), "an existing table is the answer; nothing is created")

	_, err = b.CreateTable(context.Background(), ref, backend.CreateTableOptions{Columns: cols})
	require.True(t, serr.Is(err, serr.AlreadyExists), "got %v", err)

	for name, tc := range map[string]struct {
		opts backend.CreateTableOptions
		want serr.Code
	}{
		"no columns and no query":   {backend.CreateTableOptions{}, serr.InvalidArgument},
		"columns and a query":       {backend.CreateTableOptions{Columns: cols, AsSelect: "SELECT 1"}, serr.InvalidArgument},
		"two partition columns":     {backend.CreateTableOptions{Columns: cols, PartitionBy: []string{"a", "b"}}, serr.Unsupported},
		"a column of unknown type":  {backend.CreateTableOptions{Columns: []backend.Column{{Name: "x", Type: backend.TypeUnknown, NativeType: "RANGE<DATE>"}}}, serr.Unsupported},
		"an array without elements": {backend.CreateTableOptions{Columns: []backend.Column{{Name: "x", Type: backend.TypeArray}}}, serr.InvalidArgument},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := b.CreateTable(context.Background(), backend.TableRef{Dataset: "d", Table: "other"}, tc.opts)
			require.True(t, serr.Is(err, tc.want), "got %v", err)
		})
	}
}

func TestCreateTableAsSelectRunsTheQueryIntoTheNewTable(t *testing.T) {
	f := newFake(t)
	f.handle("POST "+jobsPath, 200, jobResource("DONE", nil, nil))
	f.handle("GET "+jobPath, 200, jobResource("DONE", nil, nil))
	f.handle("GET "+resultsPath, 200, resultsResource([]map[string]any{field("id", "INTEGER", "NULLABLE")}))
	f.handle("GET "+tablePath, 200, map[string]any{"tableReference": tableRef("d", "t"), "type": "TABLE",
		"schema": map[string]any{"fields": []map[string]any{field("id", "INTEGER", "NULLABLE")}}})
	b := f.open(t, backend.Config{DefaultDataset: "src", MaxQueryBytes: 1 << 30})

	got, err := b.CreateTable(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, backend.CreateTableOptions{
		AsSelect: "SELECT id FROM source", PartitionBy: []string{"_PARTITIONTIME"}, ClusterBy: []string{"id"},
	})
	require.NoError(t, err)
	require.Equal(t, "id", got.Columns[0].Name)

	query := f.seen("POST", "/jobs")[0].Body["configuration"].(map[string]any)["query"].(map[string]any)
	require.Equal(t, "SELECT id FROM source", query["query"])
	require.Equal(t, tableRef("d", "t"), query["destinationTable"])
	require.Equal(t, "WRITE_EMPTY", query["writeDisposition"], "a table that already holds rows is not overwritten")
	require.Equal(t, "CREATE_IF_NEEDED", query["createDisposition"])
	require.Equal(t, map[string]any{"type": "DAY"}, query["timePartitioning"])
	require.Equal(t, map[string]any{"projectId": "test-project", "datasetId": "src"}, query["defaultDataset"])
	require.Equal(t, "1073741824", query["maximumBytesBilled"], "the server's byte cap binds a CTAS too")
}

func TestDropTable(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("DELETE "+tablePath, 204, nil)
	f.handle("DELETE "+tablesPath+"/gone", 404, apiError(404, "notFound", "Not found: Table"))
	b := f.open(t, backend.Config{})

	require.NoError(t, b.DropTable(context.Background(), backend.TableRef{Dataset: "d", Table: "t"}, backend.DropTableOptions{}))
	require.True(t, serr.Is(b.DropTable(context.Background(), backend.TableRef{Dataset: "d", Table: "gone"}, backend.DropTableOptions{}), serr.NotFound))
	require.NoError(t, b.DropTable(context.Background(), backend.TableRef{Dataset: "d", Table: "gone"}, backend.DropTableOptions{IfExists: true}))
}

// A name is expanded into a request path without escaping, so a dataset named
// "d/tables/t" would address a table: every operation refuses such names before
// sending anything.
func TestNamesCannotChangeWhatIsAddressed(t *testing.T) {
	f := newFake(t)
	b := f.open(t, backend.Config{})
	ctx := context.Background()
	bad := []string{"d/tables/t", "d/../x", "d?x=1", "d#x", "d%2Ftables", "d.t", "", "d t"}

	for _, name := range bad {
		ref := backend.TableRef{Dataset: name, Table: "t"}
		_, err := b.GetTable(ctx, ref)
		require.True(t, serr.Is(err, serr.InvalidArgument), "GetTable %q: %v", name, err)
		_, err = b.CreateTable(ctx, ref, backend.CreateTableOptions{Columns: []backend.Column{{Name: "a", Type: backend.TypeInt64}}})
		require.True(t, serr.Is(err, serr.InvalidArgument), "CreateTable %q: %v", name, err)
		require.True(t, serr.Is(b.DropTable(ctx, ref, backend.DropTableOptions{}), serr.InvalidArgument), "DropTable %q", name)
		_, err = b.ListTables(ctx, backend.ListTablesOptions{Dataset: name})
		if name != "" { // an empty dataset means "the default", and there is none
			require.True(t, serr.Is(err, serr.InvalidArgument), "ListTables %q: %v", name, err)
		}
		if name != "" {
			_, err = b.CreateDataset(ctx, name, backend.CreateDatasetOptions{})
			require.True(t, serr.Is(err, serr.InvalidArgument), "CreateDataset %q: %v", name, err)
			require.True(t, serr.Is(b.DropDataset(ctx, name, backend.DropDatasetOptions{Cascade: true}), serr.InvalidArgument), "DropDataset %q", name)
		}
	}
	for _, name := range []string{"t/../x", "t?x", "t#", "t%2F", "t.u", "", "t\x00"} {
		_, err := b.GetTable(ctx, backend.TableRef{Dataset: "d", Table: name})
		require.True(t, serr.Is(err, serr.InvalidArgument), "table %q: %v", name, err)
	}
	require.Empty(t, f.requests, "no such name reached BigQuery")
}

func TestTableNamesMayHoldWhatBigQueryAllows(t *testing.T) {
	f := newFake(t)
	f.handle("GET /projects/test-project/datasets/d_1/tables/my-table%20two", 200, map[string]any{"tableReference": tableRef("d_1", "my-table two"), "type": "TABLE"})
	b := f.open(t, backend.Config{})
	got, err := b.GetTable(context.Background(), backend.TableRef{Dataset: "d_1", Table: "my-table two"})
	require.NoError(t, err)
	require.Equal(t, "my-table two", got.Info.Ref.Table)
}

func TestListTablesOfAMissingDatasetIsNotFound(t *testing.T) {
	quiet(t)
	f := newFake(t)
	f.handle("GET "+tablesPath, 404, apiError(404, "notFound", "Not found: Dataset test-project:d"))
	b := f.open(t, backend.Config{})

	_, err := b.ListTables(context.Background(), backend.ListTablesOptions{Dataset: "d"})
	require.True(t, serr.Is(err, serr.NotFound), "got %v", err)
}
