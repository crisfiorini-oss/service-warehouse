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

## Datasets vs. tables

The server is bound to one **database** (a BigQuery project, a Snowflake /
Redshift database, a ClickHouse server, or a DuckDB file). Within it, a
**dataset** is the namespace — a BigQuery dataset, a Snowflake / Redshift
schema, or a ClickHouse database — and a **table** is the leaf.

## Backends

| Kind | Status | Notes |
|------|--------|-------|
| `mem` | **working** | in-memory catalog + DDL, no query engine — the zero-dep default for tests |
| `bigquery` | **working** | catalog, DDL (incl. CTAS), queries with named parameters and a dry run, job get/cancel, streaming `InsertRows`; no `Load`, `Unload` or `Native` |
| `duckdb` | skeleton | intended embedded local engine (the "MinIO of warehouses") |
| `snowflake` | planned | |
| `redshift` | planned | |
| `clickhouse` | planned | |

What each wired backend serves, per RPC. Anything marked `Unimplemented` returns
that gRPC code and says so in `Capabilities`, so a client introspects first and
never discovers a gap by a wrong answer.

| RPC | `mem` | `bigquery` |
|-----|-------|------------|
| `Query` | `Unimplemented` | yes — named parameters, dry run, byte cap, timeout |
| `GetJob` / `CancelJob` | `NotFound` (it runs no jobs) | yes |
| `ListDatasets` / `ListTables` / `GetTable` | yes | yes |
| `CreateDataset` / `DropDataset` | yes | yes |
| `CreateTable` / `DropTable` | yes (CTAS: `Unimplemented`) | yes, including CTAS |
| `InsertRows` | `Unimplemented` | yes — streaming inserts, per-row refusals |
| `Load` / `Unload` | `Unimplemented` | `Unimplemented` |
| `Native` | `Unimplemented` | `Unimplemented` (no verbs) |

The next milestone is `duckdb` (SQL execution + Arrow encoding), which makes the
"develop on DuckDB, ship on BigQuery" story real; until then the zero-dependency
way to run the server is `SWH_BACKEND=mem`.

### The BigQuery backend

Credentials are Application Default Credentials, or the service-account key
named by `SWH_CREDENTIALS_FILE`; nothing else is read. The server is bound to
the project in `SWH_DATABASE`: jobs run and are billed in it, and `dataset` names
are datasets of it. What the credentials must be allowed to do follows the RPCs a
deployment uses: run query jobs and read data (`Query`, CTAS), read table
metadata (`GetTable`, `ListTables`, and `InsertRows`, which reads it to compare
the batch with the table), create and delete datasets and tables, and stream rows
into a table.

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

**Implementation decisions (2026-10-15).**

| Decision | Evidence and outcome |
|----------|----------------------|
| Build or reuse | Searched this repository and the shared `codefly-dev/core` and `codefly-dev/sdk-go` implementations: the existing BigQuery backend in [#14](https://github.com/codefly-dev/service-warehouse/pull/14) is the warehouse implementation; the shared libraries supply caller authentication and agent/runtime support, with no other BigQuery backend. Retain and extend the existing backend and `internal/arrowipc`. Go's standard library has no BigQuery or Arrow implementation; reuse Google's `cloud.google.com/go/bigquery` client (its query, inserter and per-row error implementations and tests) and Apache Arrow's IPC implementation, with the portable contract and bounded framing owned here. |
| Scope exception | On 2026-10-15 the user approved retaining #14's existing implementation, which already exceeds the 2,000 hand-written-line stop threshold in [#23](https://github.com/codefly-dev/service-warehouse/pull/23). This exception retains the documented catalog, DDL, query, job and streaming-insert behavior; it does not authorize additional features. |
| Feature order | Finish and merge #14 first, then the DuckDB engine, then restricted `Scan`, then export the shared Arrow helpers. Open each dependent PR only after its base has merged. `Scan` is absent from this version's contract; the backend's query path uses REST results and creates query jobs. |

## Configuration (env)

| Var | Default | Notes |
|-----|---------|-------|
| `SWH_LISTEN` | `:9465` | gRPC listen address |
| `SWH_BACKEND` | `duckdb` | `mem` \| `duckdb` \| `bigquery` \| `snowflake` \| `redshift` \| `clickhouse` |
| `SWH_DATABASE` | — | required for cloud backends (BQ project / DB name); DuckDB path |
| `SWH_DATASET` | — | default namespace for unqualified names (for BigQuery also the default dataset of a query's SQL) |
| `SWH_LOCATION` | — | region for datasets/jobs (BigQuery: where jobs run and where a dataset is created when the request names none) |
| `SWH_DSN` | — | backend-native connection string (overrides discrete fields); `bigquery` has none and refuses it |
| `SWH_HOST` / `SWH_PORT` | — | SQL backend host/port; `bigquery` refuses them |
| `SWH_USER` / `SWH_PASSWORD` | — | credentials; `bigquery` refuses them |
| `SWH_ACCOUNT` | — | Snowflake account identifier; `bigquery` refuses it |
| `SWH_CREDENTIALS_FILE` | — | BigQuery service-account JSON (else ADC) |
| `SWH_MAX_QUERY_BYTES` | `0` | per-query scan cap (0 = backend default); for BigQuery the ceiling of `maximumBytesBilled`, which a request may only lower |
| `SWH_QUERY_TIMEOUT` | `0` | per-query timeout (Go duration; 0 = backend default); a request may only shorten it |
| `SWH_AUTH_TOKEN` | — | shared secret every caller must present; required unless `SWH_ALLOW_ANONYMOUS=true` |
| `SWH_ALLOW_ANONYMOUS` | `false` | `true` accepts callers with no token; refused together with `SWH_AUTH_TOKEN` |
| `SWH_MAX_ARROW_MESSAGE_BYTES` | `4194304` (4 MiB) | largest body of one Arrow IPC message a client may send (`InsertRows`); a larger one is `InvalidArgument`, refused before any of it is allocated. Must be positive. The default is grpc-go's own receive limit, which a message cannot exceed anyway: raising this above it has no effect until the gRPC server's limit is raised too |

The `bigquery` backend reads `SWH_DATABASE` (required), `SWH_DATASET`,
`SWH_LOCATION`, `SWH_CREDENTIALS_FILE`, `SWH_MAX_QUERY_BYTES`,
`SWH_QUERY_TIMEOUT` and `SWH_MAX_ARROW_MESSAGE_BYTES`, and adds no variable of its
own. A setting it never reads is a startup error, not something silently ignored.

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
to the gateway's unsupported `duckdb` default and startup fails. The agent's
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
v0.19.4 exposes no ConfigMap-reference input; it cannot render a ConfigMap-key
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
`docker build`/`docker buildx`/`docker/build-push-action`, or calling core's
shared `publish-service-image`) or deploys one (a
Kubernetes manifest, Helm values, a Kustomize overlay naming an image), and fails
on any that is not accounted for, or on one that stopped producing its signal,
or when the tests holding the evidence are gone. A second image therefore has to
arrive with its evidence and with an edit to that list.

The gate is scoped to this repository, which is the limit of what it can prove.
If another repository ever packages this binary into an image, the service ships
an image that nothing here can see, and that case has to be recorded where that
image is built.

### Releasing

**Who publishes the image.** GHCR checks the workflow's defining repository, so
the publishing *job* cannot move to core: core's reusable publisher was denied
`installation not allowed to Write organization package` when writing a service
package, and core removed it. `publish-gateway-image.yml` therefore keeps the
job here — the `workflow_dispatch` gate, `packages: write`, the 30-minute
timeout and the checkout — and the publishing *steps* are core's shared
composite action [`publish-service-image`][publisher], called as a step inside
that job and pinned to the core commit `go.mod` requires. The action logs in
with this job's own `GITHUB_TOKEN`, builds the committed `Dockerfile` with the
repository root as context, and pushes by digest with no tags. Before login or
build it refuses an event other than `workflow_dispatch`, a commit not reachable
from the remote's own default branch, a `HEAD` that is not the triggering commit,
a tree with any modified, staged, untracked or ignored file, local Git config
carrying credentials, Git config-defined hooks or filter drivers, any path with a
`filter` attribute, and submodules (this repository has no `.gitattributes` and
no submodules) — which is why the checkout uses `persist-credentials: false`
and full history, and why nothing but the cgo cross-build emulator runs ahead of
it. `registry-token` is the one place this job's token goes.

The emulator (`docker/setup-qemu-action`) stays in this repository: the gateway
is a cgo build, so the `linux/arm64` stage runs an arm64 toolchain rather than
cross-compiling, and the action sets up buildx but installs no binfmt handler.
It is the only step before the action, it runs no code from this repository and
writes nothing into the checkout, so the action's guard still covers the tree it
builds.

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
   It pushes by digest and writes `gateway-image.json`, which core's action
   uploads as the artifact **`gateway-image.json-0.0.2`** — `<lock-file>-<version>`,
   not the `gateway-image-lock-<version>` the in-repository publisher used — and
   prints in the run summary. The run's `digest` and compact `lock` are also job
   outputs. A dispatch on any other branch is refused at the ancestry check
   before registry login; so is a dirty or ref-overridden checkout.
4. Make the new `ghcr.io/codefly-dev/service-warehouse` package **public** in
   GitHub package settings. Verify an anonymous pull by the returned digest
   using a fresh Docker configuration with no registry login.
5. Commit the returned lock in a reviewed PR. Let CI inventory both declared
   platforms (`linux/amd64`, `linux/arm64`), then merge that PR.
6. Run the full `codefly agent ci` on synced `main`, including its audit and
   generated-service `codefly ci run`. Done for v0.0.2 (run 38167325616); since
   then `agent-ci.yml` runs on every pull request.
7. Confirm the repository's `GH_PAT` release secret is configured, and retain
   source packager `codefly.dev/go:0.0.69` and the patched Go 1.27.3 toolchain.
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
use `sha256:` followed by 64 lowercase hex characters. The action records
whatever digest the build returned without checking that shape itself — the
in-repository publisher used to — so the check that matters is the one on the
commit: `go test ./...` parses the committed lock on every pull request, and CI
then inventories the digest it names. An empty digest records
an unpublished image and is refused by the agent and release. CI permits the
initial empty lock only with complete history, no `v*` tag, and no prior nonempty
lock in fetched history. Clearing an established lock fails CI even before the
first tag.

[sbom]: https://github.com/codefly-dev/core/blob/v0.19.4/docs/sbom.md
[publisher]: https://github.com/codefly-dev/core/blob/v0.19.4/.github/actions/publish-service-image/action.yml

## Develop

```bash
# regenerate stubs from proto (requires the codefly CLI + Docker)
codefly generate proto --proto ./proto --output ./gen

go build ./...
go vet ./...
go test -race ./...      # unit tests: mem, a bufconn server, the agent, and the
                         # bigquery backend against a fake of BigQuery's REST API
```

The release-gate regression tests use `git` and `jq` (also present in CI).

The source packager is pinned to `codefly.dev/go:0.0.69`, whose image recipes use
immutable digests. `go.mod` and CI require Go 1.27.3; the gateway Dockerfile pins
the Go 1.27.3 image index and sets `GOTOOLCHAIN=go1.27.3`. For local conformance,
run `GOTOOLCHAIN=go1.27.3 codefly agent ci` with CLI **0.1.185**, as pinned in
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

The `bigquery` unit tests contact nothing: they run the real client against a
fake of BigQuery's REST API served from the test process, so they check what the
backend sends and how it reads an answer, not what BigQuery does. The one test
that talks to BigQuery, `TestAgainstRealBigQuery` in
[`internal/backend/bigquery/integration_test.go`](internal/backend/bigquery/integration_test.go),
is skipped unless `SWH_TEST_BIGQUERY_PROJECT` names a project it may use. It
creates a scratch dataset of its own with a random name, works only inside it, and
deletes it. `SWH_TEST_BIGQUERY_LOCATION` and `SWH_TEST_BIGQUERY_CREDENTIALS_FILE`
are optional (Application Default Credentials otherwise). CI runs no BigQuery, so
that test has not run in CI.

The current live test proves streamed inserts are visible to `Query`, when run.
It cannot prove visibility through the Storage Read API because `Scan` is a
later feature. The `Scan` implementation must extend this live test to read a
streamed row through a read session, accept `IN` and timestamp comparisons in
the session's row restriction, and verify that those filters select the expected
rows. A REST fake or a successful `Query` is not evidence for that requirement.

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
