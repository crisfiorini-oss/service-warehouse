# service-warehouse

A **server exposing a generic data-warehouse API** over a uniform gRPC contract,
backed by **BigQuery / Snowflake / Redshift / ClickHouse**, with **DuckDB** as
the embedded local/test engine.

Clients speak only the gRPC API and **never link a warehouse SDK**. All
provider-specificity lives in this one server — written once, all backends
compiled in and selected by config.

This is the warehouse analog of
[`service-object-storage`](https://github.com/codefly-dev/service-object-storage):
that gateway abstracts S3 / GCS / Azure / MinIO behind one object API; this one
abstracts the SQL warehouses behind one query + catalog API.

## Why

Every app that talks to a warehouse otherwise links a heavy vendor SDK, holds
vendor credentials, and hard-codes one vendor's dialect and result format. This
replaces that: the app talks to one gateway, and the backend is a per-environment
choice — **local/test runs DuckDB**, deployed names the real warehouse. Because
the app only ever speaks the uniform API, you **develop on DuckDB and ship on
BigQuery**; the gap is absorbed and tested once, here, not in every app.

## The API (`codefly/warehouse/v0`)

The honest intersection across the SQL warehouses (SQL in, Arrow out — the model
BigQuery Storage / Snowflake / ADBC converged on). Anything backend-specific
(time-travel, clustering ops, warehouse resize, …) is reached only through
`Native`.

| RPC | Purpose |
|-----|---------|
| `Query` | submit SQL (+ bind params, dry-run, byte cap); stream **Arrow** result batches |
| `Scan` | read the rows of one table that match a structured filter, with no SQL and no query job; stream **Arrow** result batches |
| `GetJob` / `CancelJob` | inspect / cancel an async job |
| `ListDatasets` / `ListTables` / `GetTable` | catalog introspection |
| `CreateDataset` / `DropDataset` | namespace DDL |
| `CreateTable` / `DropTable` | table DDL (incl. CTAS) |
| `Load` | bulk-load object-storage files (CSV/JSON/Parquet/Avro/ORC) into a table |
| `Unload` | export a table or query to object storage |
| `InsertRows` | streaming Arrow row appends |
| `Capabilities` | machine-readable feature set — introspect before calling |
| `Native` | escape hatch for backend-specific verbs |

Errors are normalized to gRPC status codes (`NotFound`, `AlreadyExists`,
`FailedPrecondition`, `Unimplemented`, `ResourceExhausted`, `DeadlineExceeded`,
…) so clients never parse a vendor SQLSTATE or reason string. The text of a
failure is written by this server; the one piece of vendor text a client can
read is the diagnosis of its own invalid SQL (`InvalidArgument`).

## Results are Arrow

`Query` streams a header (portable result schema + the equivalent Arrow-IPC
schema) followed by Arrow-IPC record batches. This is the encoding the industry
converged on for warehouse egress, so a client feeds batches straight into any
Arrow reader — no per-vendor row shape.

## Scan: rows without SQL

`Scan` reads the rows of one table that match a structured filter, and answers as
`Query` does (a header, then Arrow batches). It exists for a deployment whose
identity may read a table but may not run a query job (on BigQuery, one that holds
read sessions and not `bigquery.jobs.create`), and a backend that offers it says so
in `Capabilities.table_scan`; elsewhere it is `Unimplemented`.

```
ScanRequest { TableRef table; repeated string selected_fields; Filter filter; }
Filter      { repeated Predicate predicates; }          // ANDed
Predicate   { string field; PredicateOp op; ColumnType type; repeated string values; }
PredicateOp   EQ | IN | LT | LE | GT | GE | IS_NULL | IS_NOT_NULL
```

A predicate is data and never SQL text: `field` is a column of the table, `op` takes
one value (`EQ`, `LT`, `LE`, `GT`, `GE`), one or more (`IN`) or none (`IS_NULL`,
`IS_NOT_NULL`, which also take no `type`), and `values` are text in the form
`QueryParam.value` takes for `type`. An empty `selected_fields` returns every
column in the table's order, otherwise the named columns in that order; an empty
filter reads every row. A column the table lacks, a column selected twice, a
malformed value and a wrong number of values are `InvalidArgument`. The header's
`job` has no `id`; where a backend can say before it reads what a scan will read,
`job.stats.bytes_scanned` is that estimate.

## Datasets vs. tables

The server is bound to one **database** (a BigQuery project, a Snowflake /
Redshift database, a ClickHouse server, or a DuckDB file). Within it, a
**dataset** is the namespace — a BigQuery dataset, a Snowflake / Redshift
schema, or a ClickHouse database — and a **table** is the leaf.

## Backends

| Kind | Status | Notes |
|------|--------|-------|
| `mem` | **working** | in-memory catalog + DDL, no query engine — the zero-dep default for tests |
| `duckdb` | **working** | embedded local engine: SQL, Arrow results, DDL and streaming insert, in-process (see [The DuckDB engine](#the-duckdb-engine)) |
| `bigquery` | **working** | catalog, DDL (incl. CTAS), queries with named parameters and a dry run, job get/cancel, streaming `InsertRows`; no `Load`, `Unload` or `Native` |
| `snowflake` | planned | |
| `redshift` | planned | |
| `clickhouse` | planned | |

What each wired backend serves, per RPC. Anything marked `Unimplemented` returns
that gRPC code and says so in `Capabilities`, so a client introspects first and
never discovers a gap by a wrong answer.

| RPC | `mem` | `duckdb` | `bigquery` |
|-----|-------|----------|------------|
| `Query` | `Unimplemented` | yes — one statement, `$name` parameters, dry run, timeout | yes — named parameters, dry run, byte cap, timeout |
| `Scan` | `Unimplemented` | yes — a `SELECT` whose values are all bound parameters | yes — a Storage Read API session, no query job |
| `GetJob` / `CancelJob` | `NotFound` (it runs no jobs) | `Unimplemented` | yes |
| `ListDatasets` / `ListTables` / `GetTable` | yes | yes | yes |
| `CreateDataset` / `DropDataset` | yes | yes | yes |
| `CreateTable` / `DropTable` | yes (CTAS: `Unimplemented`) | yes, including CTAS | yes, including CTAS |
| `InsertRows` | `Unimplemented` | yes — per-row refusals | yes — streaming inserts, per-row refusals |
| `Load` / `Unload` | `Unimplemented` | `Unimplemented` | `Unimplemented` |
| `Native` | `Unimplemented` | `Unimplemented` | `Unimplemented` (no verbs) |

`duckdb` is the default, and it is the engine that makes "develop on DuckDB, ship
on BigQuery" real: a client runs the same SQL and reads the same Arrow types on
both. `mem` serves the whole catalog/DDL plane and returns `Unimplemented` for
query and data-movement, so a client introspects `Capabilities` and never
discovers a gap by a wrong answer — and so does every `duckdb` call it does not
offer.

### The BigQuery backend

Credentials are Application Default Credentials, or the service-account key
named by `SWH_CREDENTIALS_FILE`; nothing else is read. The server is bound to
the project in `SWH_DATABASE`: jobs run and are billed in it, and `dataset` names
are datasets of it. What the credentials must be allowed to do follows the RPCs a
deployment uses: run query jobs and read data (`Query`, CTAS), read table
metadata (`GetTable`, `ListTables`, `InsertRows`, which reads it to compare the
batch with the table, and `Scan`, which reads it to check the request against the
table), create and delete datasets and tables, stream rows into a table, and, for
`Scan`, create read sessions (`bigquery.readsessions.create`) and read table data
(`bigquery.tables.getData`). `Scan` needs no `bigquery.jobs.create`, so an
identity that may read a table but may not run SQL can use it.

**Queries.** `sql` uses BigQuery's named parameters, `@name`; a parameter's `name`
is given without the sigil. `value` is text, checked here and bound by BigQuery
with the type you give, never spliced into the SQL: booleans as `true`/`false`,
integers and floats as decimal text, `NUMERIC` as a plain decimal (it becomes
`BIGNUMERIC` when only that holds it, and a value that would be rounded is
refused), bytes as standard base64, dates as `YYYY-MM-DD`, `TIME` and wall-clock
timestamps without a zone, instants as RFC 3339 with a zone, intervals as
`Y-M D H:M:S[.F]`, JSON and geography (WKT) as text. Array and struct parameters
are `Unimplemented`; sub-microsecond precision is refused, not truncated. The
query has finished by the time its header is sent (the schema is known only
then), so `CancelJob` cannot reach a running query: cancel the call, which also
cancels the job. The job `id` is opaque; hand it back to `GetJob`/`CancelJob`.
`max_bytes_billed` and `timeout_ms` are tightened by `SWH_MAX_QUERY_BYTES` and
`SWH_QUERY_TIMEOUT`: the smaller of the two applies. A `DML`/`DDL` statement
returns a header with an empty schema and `rows_affected`.

**Results.** The header's `arrow_ipc_schema` is one Arrow IPC *Schema message*, and
each `arrow_batch` is one IPC *RecordBatch message* with no schema, up to 8192
rows or about 1 MiB of text and bytes. The schema followed by the batches is a
valid Arrow IPC stream, so a client concatenates them and reads with any Arrow
reader. Types map as `BOOL`→bool, `INT64`→int64, `FLOAT64`→float64,
`NUMERIC`→decimal128 (`BIGNUMERIC`→decimal256), `STRING`/`JSON`/`GEOGRAPHY`→utf8,
`BYTES`→binary, `DATE`→date32, `TIME`→time64[us], `DATETIME`→timestamp[us],
`TIMESTAMP`→timestamp[us, UTC], `INTERVAL`→month-day-nano interval,
`ARRAY`→list, `STRUCT`→struct. A column with no portable bucket (BigQuery
`RANGE`) is `UNKNOWN` in the catalog with its spelling in `native_type`, and a
query that returns one is `Unimplemented`.

**Scan.** `Scan` opens a Storage Read API session, which is not a query job.
`selected_fields` become the session's selected fields and the filter becomes its
row restriction, so BigQuery prunes partitions and blocks before it sends a row.
The restriction is written by this server from the structured filter: a column is
named as the table spells it, in backticks, and must exist and be a scalar
(`BOOL`, `INT64`, `FLOAT64`, `NUMERIC`, `STRING`, `BYTES`, `DATE`, `TIME`,
`DATETIME`, `TIMESTAMP`; an `ARRAY`, `STRUCT` or `JSON` column is `Unimplemented`);
a string is written as a double-quoted literal with `"` and `\` escaped and every
other non-printable-ASCII character as a `\U` escape (a NUL is refused); bytes as
a `b"\x.."` literal; a date, time, wall-clock timestamp or `NUMERIC` as a cast of
its normalized text; an instant as a cast of its UTC time with an explicit
`+00:00`. A value is parsed first, by the rules of a query parameter of its type.
A value whose type cannot be compared with the column (a string with an integer, a
`DATETIME` with a `TIMESTAMP`) is `InvalidArgument`; the numbers compare with each
other. Columns come back in the order of `selected_fields`, not the table's.

The session is split into at most `SWH_SCAN_STREAMS` streams (default 4), read in
parallel and merged into one result: rows of one stream keep their order, streams
interleave. A stream that breaks (`Unavailable`, `Internal`, `Aborted`,
`DeadlineExceeded`, `ResourceExhausted`) is reopened at the row it reached, so no
row repeats or goes missing; five failures in a row without a row in between fail
the scan. Each response is cut into batches of at most 8192 rows or about 1 MiB,
and a batch whose message still passes `SWH_MAX_ARROW_MESSAGE_BYTES` is cut again;
a single row over that limit fails the scan `Unimplemented`. The header carries
`job.stats.bytes_scanned`, the session's estimate of what it will scan, before any
batch, and a scan whose estimate is over `SWH_MAX_QUERY_BYTES` is refused
`ResourceExhausted` before it reads. It is an estimate from table metadata, not a
meter. `SWH_QUERY_TIMEOUT` bounds the whole scan. A view is `Unimplemented`: it
cannot be read without a query. A refusal by BigQuery is mapped as every other
(`PermissionDenied` stays `PermissionDenied`). Rows still in a table's streaming
buffer are visible to a scan only if the Read API shows them.

**Catalog.** Listings carry BigQuery's names only, so each entry's detail is a
metadata request of its own; pages default to 50 entries and never exceed 500.
`partition_by` is one column at day granularity (`_PARTITIONTIME` for ingestion
time); more than one is `Unimplemented`. Names are checked against what BigQuery
allows before they are used.

**`InsertRows`.** Arrow comes in the same framing: the header's schema message
and one RecordBatch message per batch, or, with no schema in the header, a first
message that is a schema message followed by its batch. A message that is not
Arrow (cut short, empty, a second schema where a batch belongs), that states a
size over a limit (metadata over 1 MiB, a body over `SWH_MAX_ARROW_MESSAGE_BYTES`),
or whose schema states more fields, children or metadata than its bytes can hold,
is `InvalidArgument`. Rows are sent as streaming inserts of at most 500 rows and
about 8 MiB. The call is not atomic: BigQuery is asked to skip invalid rows, so
valid rows are stored and refused ones come back as `errors`, each with its
`row_index` (counted from 0 across every batch of the call) and a `reason`:

| `RowRefusal` | meaning | set by `bigquery` when |
|--------------|---------|------------------------|
| `INVALID_VALUE` | a value cannot be stored in its column | BigQuery refuses the row's content, or a value cannot be encoded (text that is not UTF-8, an out-of-range time) |
| `ROW_TOO_LARGE` | the row alone is bigger than a row may be | the row passes 8 MiB as BigQuery encodes it |
| `SCHEMA_MISMATCH` | the table, not the row, disagrees | the batch has a column the table lacks, or lacks a required column; every row of the batch is refused and none is sent |
| `UNSPECIFIED` | refused, no reason stated | not set by `bigquery` |

A failure that says nothing against a row (a throttle, a quota, a timeout, a dead
backend) fails the call with a status code instead and is never a row refusal;
rows of earlier requests of that call may already be stored, so a retry can store
a row twice — delivery is at least once. A schema mismatch is found by comparing
the batch's columns with the table's metadata, so no vendor text decides it; a
table altered while the call runs can surface as `INVALID_VALUE`. An Arrow type
BigQuery has no equivalent of (map, duration, dictionary) is `Unimplemented`.

## The DuckDB engine

`SWH_BACKEND=duckdb` runs DuckDB inside the server process, through the
[`duckdb/duckdb-go`](https://github.com/duckdb/duckdb-go) driver (cgo). Nothing
else has to be running. `SWH_DATABASE` is a database file; empty or `:memory:` is
an in-memory database. `SWH_DSN` is DuckDB's own DSN (`path?option=value`) and
replaces `SWH_DATABASE`.

| Call | DuckDB |
|------|--------|
| `Query` | one statement per request: `SELECT`, `EXPLAIN`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`. Anything else (`SET`, `ATTACH`, `COPY`, `PRAGMA`, transactions, `RETURNING`, a second statement) is refused. A query's job is already `DONE` when the header is sent; there are no jobs to look up |
| `Query` `dry_run` | the statement is prepared, never run: syntax, names and types are checked and the result schema is returned. Nothing is priced |
| `Query` params | DuckDB's `$name`. `@name` and `:name` are not rewritten (a rewrite cannot tell them from `::` casts and slices), and a parameter the SQL never reads is an error. Typed: BOOL, INT64, FLOAT64, STRING, DATE, TIME, TIMESTAMP (no zone), TIMESTAMPTZ (RFC 3339). An untyped parameter is text that DuckDB casts to what the SQL expects. NUMERIC, BYTES, INTERVAL, JSON, ARRAY, STRUCT parameters are `Unimplemented` (pass a STRING and cast it in the SQL) |
| catalog and DDL | a dataset is a schema of the opened database; a table is a table or a view. Names compare without regard to case, as in DuckDB. `NumRows` is the engine's estimate; sizes and timestamps are not tracked and stay 0 |
| `InsertRows` | columns are matched by name; an omitted column takes its default or NULL. See below |
| `Scan` | `SELECT <the columns> FROM <the table> WHERE <predicates ANDed>`, every column name quoted and checked against the table (a name it lacks is `InvalidArgument`), every value a bound parameter and never spliced into the SQL. The values are typed as `Query` parameters are, so `NUMERIC`, `BYTES`, `INTERVAL`, `JSON`, `ARRAY` and `STRUCT` values are `Unimplemented`. A table or a view can be scanned. It runs through `Query`'s result path, so encoding, type rules and `SWH_QUERY_TIMEOUT` are the same. The server **refuses to start** with `SWH_SCAN_STREAMS` set: one connection reads a table, so there are no streams to set |
| `GetJob`, `CancelJob`, `Load`, `Unload`, `Native` | `Unimplemented` |
| `max_bytes_billed`, `SWH_MAX_QUERY_BYTES` | DuckDB bills nothing and cannot meter a scan. The request field is `Unimplemented`, and the server **refuses to start** with `SWH_MAX_QUERY_BYTES` set rather than run uncapped |
| `timeout_ms`, `SWH_QUERY_TIMEOUT` | the shorter of the two applies, and the statement is interrupted; it fails `DeadlineExceeded` |

Options a dataset or a table has elsewhere and DuckDB has not — a location,
labels, a description, partitioning, clustering — are `Unimplemented`, not
accepted and dropped. So is a `GEOGRAPHY` column (DuckDB's `GEOMETRY` is planar).
A column whose `nullable` is unset is `NOT NULL`, as the contract spells it. A
configured `SWH_DATASET` must exist: unqualified names resolve in it, and running
against a search path that is missing would resolve them somewhere else.

### Types

Every integer width is `INT64` and every float is `FLOAT64`, in the header's
portable schema and in the Arrow batches, so a client reads the same Arrow type
here as on a backend that has one width. `native_type` keeps DuckDB's spelling.

| DuckDB | Portable | Arrow |
|--------|----------|-------|
| `BOOLEAN` | BOOL | `bool` |
| `TINYINT` … `BIGINT`, `UTINYINT` … `UINTEGER` | INT64 | `int64` |
| `UBIGINT` | NUMERIC(20,0) | `decimal128(20,0)` |
| `HUGEINT`, `UHUGEINT` | NUMERIC(38,0); a value of more than 38 digits is refused | `decimal128(38,0)` |
| `FLOAT`, `DOUBLE` | FLOAT64 | `float64` |
| `DECIMAL(p,s)` | NUMERIC(p,s) | `decimal128(p,s)` |
| `VARCHAR`, `ENUM` | STRING | `utf8` |
| `BLOB` | BYTES | `binary` |
| `DATE` | DATE | `date32` |
| `TIME` | TIME | `time64[us]` |
| `TIMESTAMP`, `TIMESTAMP_S`, `TIMESTAMP_MS` | TIMESTAMP | `timestamp[us]` |
| `TIMESTAMP_NS` | TIMESTAMP | `timestamp[ns]` |
| `TIMESTAMPTZ` | TIMESTAMPTZ | `timestamp[us, UTC]` |
| `INTERVAL` | INTERVAL | `month_day_nano_interval` |
| `LIST`, `ARRAY` | ARRAY | `list` |
| `STRUCT` | STRUCT | `struct` |
| `UUID`, `TIMETZ`, `BIT`, `BIGNUM`, `GEOMETRY` | UNKNOWN | `utf8` text (`binary` for `GEOMETRY`) |
| `JSON` | JSON — **a result column is refused**, see below | |
| `MAP`, `UNION` | not returned | |

`JSON` is stored and described, and `InsertRows` writes the text as given (compacted), but a query
that returns a JSON column fails `Unimplemented`: the driver decodes JSON into Go
values before this server sees it, which rounds integers above 2^53 and reads a
JSON `null` as SQL `NULL`. `CAST(col AS VARCHAR)` returns the stored text.

### Arrow on the wire

`arrow_ipc_schema` is an Arrow IPC *schema message* and every batch is an Arrow
IPC *record-batch message* (the encapsulated form: continuation marker, length,
metadata, body). The schema message followed by the batch messages is a valid
Arrow IPC stream, so a client feeds them to any Arrow reader; `internal/arrowipc`
does the framing for both directions. `InsertRows` takes the same two parts; with
no header schema, the first batch message must carry it.

### InsertRows

The insert is not atomic and says which rows it refused. A batch is written in one
transaction; if the engine refuses it, each of its rows is tried alone, so the
accepted rows are in the table and each refused row is reported by its offset in
the stream (`RowError.row_index`). A row is refused for a constraint, or for a
value its column cannot hold — out of range, too many digits, not JSON, finer
than a microsecond, not a member of the `ENUM`. The message is the server's own
and never the engine's. A failure that is not about a row (the table is gone, the
stream broke) fails the call.

### SQL cannot reach the host

This server authorizes nothing, so the engine is opened closed: SQL cannot read or
write host files (`read_csv`, `COPY`, `ATTACH` of a file), cannot load or install
extensions, and cannot change the engine's configuration (`lock_configuration`).
`SWH_DSN` may not set these options, and the server reads them back at startup and
refuses to run if they did not apply. Each request gets its own connection, so no
session state (a temporary table, a `USE`) leaks from one request to another.

### Errors

DuckDB's failures arrive as structured exception types and are mapped to codes;
the message a client sees is the server's own, never DuckDB's, which quotes the
caller's SQL and host paths. A missing table or function is `NotFound`; an
existing one is `AlreadyExists`; bad SQL, a bad cast or an out-of-range value is
`InvalidArgument`; a constraint or a dependency is `FailedPrecondition`; reaching
for the host is `PermissionDenied`. DuckDB gives no structured signal for a missing
schema in a qualified name, so that is `InvalidArgument`.

## Configuration (env)

| Var | Default | Notes |
|-----|---------|-------|
| `SWH_LISTEN` | `:9465` | gRPC listen address |
| `SWH_BACKEND` | `duckdb` | `mem` \| `duckdb` \| `bigquery` \| `snowflake` \| `redshift` \| `clickhouse` |
| `SWH_DATABASE` | — | required for cloud backends (BQ project / DB name); for DuckDB a database file, empty = in memory |
| `SWH_DATASET` | — | default namespace for unqualified names (for BigQuery also the default dataset of a query's SQL; for DuckDB it must exist) |
| `SWH_LOCATION` | — | region for datasets/jobs (BigQuery: where jobs run and where a dataset is created when the request names none) |
| `SWH_DSN` | — | backend-native connection string (overrides discrete fields); `bigquery` has none and refuses it; for DuckDB `path?option=value` |
| `SWH_HOST` / `SWH_PORT` | — | SQL backend host/port; `bigquery` refuses them |
| `SWH_USER` / `SWH_PASSWORD` | — | credentials; `bigquery` refuses them |
| `SWH_ACCOUNT` | — | Snowflake account identifier; `bigquery` refuses it |
| `SWH_CREDENTIALS_FILE` | — | BigQuery service-account JSON (else ADC) |
| `SWH_MAX_QUERY_BYTES` | `0` | per-query scan cap (0 = backend default); for BigQuery the ceiling of `maximumBytesBilled`, which a request may only lower; DuckDB cannot meter bytes and refuses to start if it is set |
| `SWH_QUERY_TIMEOUT` | `0` | per-query timeout (Go duration; 0 = backend default); a request's `timeout_ms` can only shorten it |
| `SWH_AUTH_TOKEN` | — | shared secret every caller must present; required unless `SWH_ALLOW_ANONYMOUS=true` |
| `SWH_ALLOW_ANONYMOUS` | `false` | `true` accepts callers with no token; refused together with `SWH_AUTH_TOKEN` |
| `SWH_MAX_ARROW_MESSAGE_BYTES` | `4194304` (4 MiB) | largest body of one Arrow IPC message a client may send (`InsertRows`); a larger one is `InvalidArgument`, refused before any of it is allocated. Also the largest batch message `bigquery`'s `Scan` sends: it cuts a larger batch in two. Must be positive. The default is grpc-go's own receive limit, which a message cannot exceed anyway: raising this above it has no effect until the gRPC server's limit is raised too |
| `SWH_SCAN_STREAMS` | `0` | most parallel streams one `Scan` reads (`bigquery`: a read session is split into at most this many; `0` is its default of 4). Between 0 and 1000, the Storage Read API's own ceiling; anything else is a startup error. `duckdb` has no streams and refuses to start with it set |

The `bigquery` backend reads `SWH_DATABASE` (required), `SWH_DATASET`,
`SWH_LOCATION`, `SWH_CREDENTIALS_FILE`, `SWH_MAX_QUERY_BYTES`,
`SWH_QUERY_TIMEOUT`, `SWH_MAX_ARROW_MESSAGE_BYTES` and `SWH_SCAN_STREAMS`. A setting it never reads is a startup error, not something silently ignored.

## Authentication

The server holds the warehouse credentials and runs SQL against the database it
is bound to, and the listen address says nothing about who can reach it. So it
**refuses to start on an unauthenticated listener** unless the operator says
otherwise: `SWH_AUTH_TOKEN` must be set, or `SWH_ALLOW_ANONYMOUS` must be `true`
as an explicit local-development opt-in. The generated deployment requires
authentication and never enables anonymous access.

With `SWH_AUTH_TOKEN` set, every RPC — unary and streaming, `Capabilities`
included — must carry the secret as the `x-codefly-token` gRPC metadata header,
the key the Codefly host already uses to authenticate agent plugins. Anything
else is rejected with `UNAUTHENTICATED` before it reaches a backend. The token
is compared in constant time and never logged by core's `callertoken` package.
Only `grpc.health.v1.Health/Check` and `/Watch` are exempt; `/List` still needs
the token. These probes report that the process is serving, not that a warehouse
is reachable. An interceptor configured with an empty server token fails closed;
the explicit anonymous mode installs no token interceptor.

This is authentication only — whether the caller holds the deployment's token.
The server still authorizes nothing: which caller may run which statement is
settled before the request arrives.

## Running as a codefly service

A workspace composes this gateway as `codefly.dev/warehouse`, the same way it
composes [`service-object-storage`](https://github.com/codefly-dev/service-object-storage)
as `codefly.dev/object-storage`. The agent is the repo-root `main` package
([`main.go`](main.go), [`runtime.go`](runtime.go), [`builder.go`](builder.go)),
declared by [`agent.codefly.yaml`](agent.codefly.yaml); it is released as a GitHub
release asset a consumer pins by version. The gateway it runs is a container
image, pinned by digest in [`gateway-image.json`](gateway-image.json).

**What a consumer receives.** The configuration group `warehouse`, with
`connection` (`grpc://host:port`), `endpoint` (`host:port`) and `token`, a
secret value to send as the `x-codefly-token` metadata header. The agent mints a
fresh 64-character lowercase hex token (256 random bits) for every local run
unless an operator pins one, so a run revokes the previous run's.

**Locally** (`codefly run`) the agent starts the gateway container, publishes its
port, waits until the gateway reports itself serving *and* accepts the agent's
token, and hands the endpoint and token to consumers. That wait attests the
process and the credential, not the warehouse: the gateway does not probe its
backend and no RPC reports that. Set `backend:` in `service.codefly.yaml` to
choose the engine. Newly generated services declare `backend: mem` (the
catalog-only backend, with no query execution). Removing that setting falls back
to the gateway's default, the embedded `duckdb` engine, in memory. The agent's
`Test` step performs the same health and authenticated `Capabilities` check as
`Start`, so conformance's Init → Test path rejects a dead gateway. It returns an
explicit run-level `PASSED` or `FAILED` result and consistent counts; CLI 0.1.185
requires that verdict rather than inferring it from legacy status fields.
The agent delivers `SWH_BACKEND`, `SWH_DATABASE`,
`SWH_DATASET` and `SWH_LOCATION`, and refuses `SWH_CREDENTIALS_FILE`, which
nothing could project into the container. `SWH_GATEWAY_IMAGE` makes the agent
run a locally built image instead of the pinned one (`docker build -t
service-warehouse:local .`); it configures the agent, not the gateway.

**Deployed** the agent renders a Deployment, Service, dedicated ServiceAccount
and NetworkPolicy, with gRPC health probes on the gateway's overall service.
The listener requires a token. Supply `SWH_AUTH_TOKEN` through the deployment's
Kubernetes `secret_references`, referencing a secret provisioned by the
environment's secret manager; plaintext tokens in deployment requests are
refused. Both ephemeral and restricted output profiles use `secretKeyRef`:

```yaml
secret_references:
  SWH_AUTH_TOKEN:
    name: warehouse-auth
    key: token
```

Provision a random 64-character hex token once per environment and bind **the
same secret key** to each consumer's `warehouse.token` secret configuration.
For a consumer that uses Codefly's configuration carriers, its secret-reference
map names `CODEFLY__SERVICE_SECRET_CONFIGURATION__<MODULE>__<SERVICE>__WAREHOUSE__TOKEN`
(the warehouse producer's module and service, uppercased) and points to that
same `warehouse-auth` / `token` key. No token is embedded in an endpoint or
manifest. The agent does not create or rotate deployed secrets, or automatically
copy secret references between services: the environment owns both bindings.
An absent secret prevents pod startup; an absent or wrong consumer token is
rejected before any SQL reaches the backend.

The NetworkPolicy allows TCP ingress to the gateway port from pods in its own
namespace only. Enforcement requires a CNI that implements NetworkPolicy. This
is a namespace boundary, not a declared-consumer allowlist; same-namespace pods
still need the token. Cross-namespace consumers require an explicit policy
overlay. No service mesh or mTLS is installed by this agent.

The key `SWH_ALLOW_ANONYMOUS` is reserved in deployments: any caller-supplied
occurrence is refused, including secret references, templated/secret/plain
configuration, raw ConfigMap data and environment overrides, even with an empty
or `false` value. The template owns the single literal `false` entry. Core
v0.18.1 exposes no ConfigMap-reference input; it cannot render a ConfigMap-key
reference supplied through that API. A configured `SWH_CREDENTIALS_FILE` is also refused because nothing mounts that file: grant
the dedicated ServiceAccount a workload identity, or supply a mount and variable
through an external overlay. Other `SWH_*` values are bound onto the container
by name from the environment's service configuration after the render.

## Distribution and SBOM evidence

This repository publishes **one container image**, the gateway
(`ghcr.io/codefly-dev/service-warehouse`, for `linux/amd64` and `linux/arm64`),
and the agent binary that runs it. The service ships no other image: it has no
init, migration or sidecar image, and the warehouse behind it is not run by this
service.

Under the fleet image-SBOM contract ([`codefly-dev/core` `docs/sbom.md`][sbom])
that image owes evidence, and the agent serves it. `Builder.SBOM` at image scope
returns one CycloneDX inventory per shipped platform, covering OS packages and
installed application dependencies, each bound to the image digest and platform
that was scanned and attributed to the service. The scanner is core's
(`sbom.Image`, through `BuilderWrapper.SBOMImages`), and the response is checked
against core's `sbom.ValidateCoverage`.

- [`internal/gatewayimage`](internal/gatewayimage/image.go) declares the shipped
  platforms and role. Core's `sbom.ImageSubjects` derives one subject per declared
  platform; local images get their digest and platform from the Docker daemon.
- [`cmd/image-sbom`](cmd/image-sbom/main.go) is a thin entry point to core's
  `CollectImageEvidence` and `WriteImageEvidenceIndex`. A release runs it
  against the digest [`gateway-image.json`](gateway-image.json) records and
  saves them as the `gateway-image-sbom-<version>` workflow artifact before
  the image tags are created, then attaches every CycloneDX document and the
  index to the GitHub release. Release assets retain the evidence after workflow
  artifacts expire; `attach-image-evidence` must finish green before release
  verification is complete.
- CI builds the image and checks the same contract against it (`image-sbom`), and
  runs the agent against it (`agent-runtime-e2e`).
- `ValidationCapabilities.image_sbom` is **served but not advertised**: core
  holds the advertisement back until every consumer that evaluates it runs a core
  that can represent the phase, and a present `Validation` is authoritative for
  every operation it omits. `TestImageSBOMIsServedButNotAdvertised` marks it.

`TestEveryImageThisRepositoryShipsIsCovered` in
[`cmd/service-warehouse/distribution_test.go`](cmd/service-warehouse/distribution_test.go)
gates this. It lists every path that builds an image (a Dockerfile, an
`agent.codefly.yaml`, a compose file, a workflow running
`docker build`/`docker buildx`/`docker/build-push-action`) or deploys one (a
Kubernetes manifest, Helm values, a Kustomize overlay naming an image), and fails
on any that is not accounted for, or on one that stopped producing its signal,
or when the tests holding the evidence are gone. A second image therefore has to
arrive with its evidence and with an edit to that list.

The gate is scoped to this repository, which is the limit of what it can prove.
If another repository ever packages this binary into an image, the service ships
an image that nothing here can see, and that case has to be recorded where that
image is built.

### Releasing

**Publisher stopgap pending [core#770](https://github.com/codefly-dev/core/issues/770):**
GHCR checks the workflow's defining repository. Core's reusable publisher was
denied `installation not allowed to Write organization package` when publishing
a service package. `publish-gateway-image.yml` therefore defines the job here,
logs in with its own `GITHUB_TOKEN` and `packages: write`, refuses unmerged
commits before login, checks out with `persist-credentials: false`, and pushes
by digest with no tags. The public Git remote reads are anonymous; the branch
check receives no token and puts no credentials in Git's command line. Replace these local publishing steps with core 0.18.2's
composite action once core#770 is available and adopted; keep the job in this
repository. The other core integrations remain pinned to v0.18.1.

The coordinator performs the following first-release sequence. The manifest's
seed is `0.0.1`; `codefly publish` increments it, so the **first release is
`v0.0.2`** and the image publisher must receive **`0.0.2`**.

1. Squash the reviewed gateway PR after its CI is green, using an explicit
   squash body with `Refs #12`. Commit `632feea` has a closing directive, so the
   default concatenated commit-message body is unsafe. The squash message must
   omit the closing keyword `closes` before issue `#12`; keep that issue open
   through release verification.
2. On clean, synced `main`, run `codefly publish --dry-run` and confirm
   `0.0.1` → `0.0.2`.
3. Dispatch `publish-gateway-image` on `main` with `version=0.0.2` (no `v`).
   The in-repository publisher pushes by digest and records `gateway-image.json`
   in the `gateway-image-lock-0.0.2` artifact and run summary. It uses
   `persist-credentials: false` and refuses a commit outside the default branch's
   history.
4. Make the new `ghcr.io/codefly-dev/service-warehouse` package **public** in
   GitHub package settings. Verify an anonymous pull by the returned digest
   using a fresh Docker configuration with no registry login.
5. Commit the returned lock in a reviewed PR. Let CI inventory both declared
   platforms (`linux/amd64`, `linux/arm64`), then merge that PR.
6. Run the full `codefly agent ci` on synced `main`, including its audit and
   generated-service `codefly ci run`. After it has passed, enable the PR trigger
   in `agent-ci.yml`.
7. Confirm the repository's `GH_PAT` release secret is configured, and retain
   source packager `codefly.dev/go:0.0.69` and the patched Go 1.27.2 toolchain.
8. From clean, synced `main` with Docker available, run `codefly publish`.
   Verify that the tag matches `agent.codefly.yaml`, the locked image's version
   labels match `0.0.2`, image tags resolve to the locked digest, and the workflow
   attaches the image SBOM documents and index alongside the agent archives.
9. Verify the release and the production loader's archive resolution. Close
   [#12](https://github.com/codefly-dev/service-warehouse/issues/12) only then,
   with the release URL and the per-row answer below.

**Per-row error answer at this revision:** `RowError` contains `row_index` and
free-form `string error`; it has **no permanent/retryable code**. Consumers cannot
branch on retryability without parsing text. The separate API work in
[#14](https://github.com/codefly-dev/service-warehouse/pull/14) must land before
claiming otherwise. This agent PR references #12; it does not close it.

The lock is parsed by core's `resources.ParseImageLock`: it must name
`ghcr.io/codefly-dev/service-warehouse`, contain only `name` and `digest`, and
use `sha256:` followed by 64 lowercase hex characters. An empty digest records
an unpublished image and is refused by the agent and release. CI permits the
initial empty lock only with complete history, no `v*` tag, and no prior nonempty
lock in fetched history. Clearing an established lock fails CI even before the
first tag.

[sbom]: https://github.com/codefly-dev/core/blob/v0.18.1/docs/sbom.md

## Develop

```bash
# regenerate stubs from proto (requires the codefly CLI + Docker)
codefly generate proto --proto ./proto --output ./gen

go build ./...
go vet ./...
go test -race ./...      # unit tests: mem, duckdb in-process, a bufconn server, the agent, and the
                         # bigquery backend against fakes of BigQuery's REST API and of
                         # the Storage Read API's gRPC service (Scan)
```

The release-gate regression tests use `git` and `jq` (also present in CI).

The source packager is pinned to `codefly.dev/go:0.0.69`, whose image recipes use
immutable digests. `go.mod` and CI require Go 1.27.2; the gateway Dockerfile pins
the Go 1.27.2 image index and sets `GOTOOLCHAIN=go1.27.2`. For local conformance,
run `GOTOOLCHAIN=go1.27.2 codefly agent ci` with CLI **0.1.185**, as pinned in
`agent-ci.yml`. Before the first image lock, also set
`SWH_GATEWAY_IMAGE=service-warehouse:e2e` to a locally built image when verifying
the generated service's test phase. This override is only for runtime and image
SBOM testing: the audit of the service's published image still fails closed on
the absent lock. A passing binary audit alone is not full conformance.

The agent's end-to-end tests need Docker and are behind the `e2e` tag; the SBOM
one also needs `syft` on `PATH`:

```bash
docker build -t service-warehouse:e2e .
SWH_GATEWAY_IMAGE=service-warehouse:e2e go test -v -tags e2e -count=1 .
```

With `SWH_GATEWAY_IMAGE` unset they skip, and a skipped run still prints `ok`:
read `-v` for `--- PASS`.

The `bigquery` unit tests contact nothing: they run the real clients against a
fake of BigQuery's REST API and a fake of the Storage Read API's gRPC service
(`BigQueryRead`, used by `Scan`), both served from the test process, so they check
what the backend sends and how it reads an answer, not what BigQuery does. The one test
that talks to BigQuery, `TestAgainstRealBigQuery` in
[`internal/backend/bigquery/integration_test.go`](internal/backend/bigquery/integration_test.go),
is skipped unless `SWH_TEST_BIGQUERY_PROJECT` names a project it may use. It
creates a scratch dataset of its own with a random name, works only inside it, and
deletes it. `SWH_TEST_BIGQUERY_LOCATION` and `SWH_TEST_BIGQUERY_CREDENTIALS_FILE`
are optional (Application Default Credentials otherwise). CI runs no BigQuery, so
that test has not run in CI.

The `duckdb` backend is cgo: building needs a C compiler, and on Linux a C++
runtime (`g++`, for `libstdc++`). `ubuntu-latest` has both; a minimal Go image
does not. Its tests run the real engine in-process and need no database or network.

`buf` is never run on the host. `codefly generate proto` runs it inside the
versioned proto companion image (`ghcr.io/codefly-dev/proto`), so the plugin
versions and the `goimports` pass are the image's, fixed by its tag — two
machines regenerate the same bytes. `proto/buf.gen.yaml` is the config that
image runs, which is why it sits with the protos rather than at the root.

Running `buf generate` directly would resolve `protoc-gen-go` and
`protoc-gen-go-grpc` from your `PATH` at whatever versions happen to be
installed. That drift shows up only as the `// versions:` banner in each
generated file, so it reads as harmless and is easy to commit by accident.
`TestGeneratedStubsMatchPinnedGenerators` in
[`cmd/service-warehouse/generation_test.go`](cmd/service-warehouse/generation_test.go)
fails when the committed stubs carry a generator version other than the
companion's — the drift gate, without putting `buf` in CI.

## Layout

```
proto/codefly/warehouse/v0/   the uniform API
proto/buf.{yaml,gen.yaml}     buf config, read by the proto companion
gen/                          generated gRPC stubs
internal/backend/             Backend interface + mem, duckdb, bigquery (+ more cloud backends)
internal/arrowipc/            the Arrow IPC framing every backend shares (schema message, batch messages)
internal/server/              gRPC Warehouse implementation + proto↔backend mapping
internal/config/              env configuration
internal/serr/                normalized error model
cmd/service-warehouse/        the server binary
cmd/image-sbom/               writes the image's SBOM evidence (run by the release)
internal/gatewayimage/        the shipped platforms and runtime role; core derives evidence
main.go runtime.go builder.go the codefly agent (codefly.dev/warehouse) that runs the gateway
agent.codefly.yaml            the agent's manifest and version
gateway-image.json            the gateway image the agent pins, by digest
Dockerfile                    the gateway image
templates/                    the agent's README, factory files and Kubernetes manifests
```
