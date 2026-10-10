# service-warehouse

The entry point for this repo. A **gateway**: one gRPC contract for the SQL
warehouses (BigQuery / Snowflake / Redshift / ClickHouse, with DuckDB as the
embedded local engine), so a client speaks SQL in and Arrow out and never links
a vendor SDK. [README.md](README.md) owns *what* the service promises — the API
table, the backend matrix, the env surface, the distribution status.
`proto/codefly/warehouse/v0/warehouse.proto` is the contract of record. This
file owns *how to work here*; if it disagrees with the code, the code wins and
this file needs a fix in the same PR.

## Boundaries — non-negotiable

- **Owns:** all provider-specificity for the SQL warehouses — written once,
  every backend compiled into one binary and selected by config. The portable
  type bucket (`internal/backend/backend.go`), the normalized error model
  (`internal/serr/serr.go`), and the Arrow encoding of results.
- **Never contains:** anything a caller knows and this server does not — a
  tenant, a permission, a product noun, a business rule, or which application
  is asking. The server is bound to one database and authorizes nothing; the
  caller's authority is settled before the request arrives.
- **Depends on:** the warehouse drivers, the gRPC contract, and core's lightweight
  `callertoken` package. Caller authentication, image lock parsing, the SBOM
  scanner and evidence writing live in `codefly-dev/core` and are not copied here.
- **The agent is a second program in this module.** The repo-root `main`
  package (`main.go`, `runtime.go`, `builder.go`) is the codefly agent that
  lets a workspace compose the gateway; it links core's agent runtime, which
  `cmd/service-warehouse` must not (it uses only core's `callertoken`). The agent
  links no backend, so no cgo driver reaches its `CGO_ENABLED=0` release build
  (`TestAgentLinksNoBackendDriver`). Like the gateway, it authorizes nothing: it
  starts the gateway, hands out the one token it enforces, and renders the
  manifest.

Deployments require an environment-managed `SWH_AUTH_TOKEN` secret reference;
consumers bind the same key to `warehouse.token`. Reject caller-supplied
`SWH_ALLOW_ANONYMOUS` keys from every configuration source, even false/empty
values; only the template may supply its single literal false. The templates render a
same-namespace ingress NetworkPolicy. Anonymous access is only an explicit local
gateway opt-in. README owns the secret binding procedure and policy limits.

Three rules follow from that:

1. **The API is the honest intersection.** An operation goes into the uniform
   contract only when every backend can mean the same thing by it. Anything
   backend-specific is reached through `Native` — not through a new RPC, and
   not through an option one backend silently ignores.
2. **A gap is `Unimplemented`, never a wrong answer.** `mem` serves the whole
   catalog/DDL plane and returns `serr.Unsupported` for query and data
   movement, which `internal/server/errors.go` maps to gRPC `Unimplemented`.
   A client introspects `Capabilities` and is never told something false. Do
   not paper a gap over with a plausible empty result.
3. **Clients branch on codes, never on strings.** Every backend error is
   mapped into a `serr.Code`. Never let a vendor SQLSTATE or reason string
   reach a client, and never parse one to make a decision here.

## How to behave when something does not work

Fleet standard, tracked by `obin-ai/handbook#68`. These are not style
preferences; each is a rule an agent broke at real cost.

1. **A gap in the tooling is a bug in the tooling** — never a reason to reach
   around it. Not as a "workaround", not "just this once", not "until the
   backend lands". `gen/` is generated: if a stub is wrong, fix the proto or
   the generator wiring, never hand-edit the output. If a gate is wrong, fix
   the gate.
2. **Never hack. Always provide the best fix, even when it spans repos.** The
   right fix living in `codefly-dev/core` or `codefly-dev/cli` is not a reason
   to work around it here — open the pull request there. If it genuinely
   cannot be fixed now, the deliverable is a precise issue against the owner
   plus an explicitly labelled stopgap, never an unlabelled one.
3. **Classify every change that makes something work**, in the PR body: a
   *fix* at the place that owns the behaviour, or a *hack*. A hack does not
   become a fix by working, by being small, by being local, or by the real fix
   belonging to someone else.
4. **Never hardcode what the system resolves.** `internal/config/config.go` is
   the entire gateway configuration surface and every knob is an `SWH_*`
   variable — a listen address, a DSN, a credentials path, a query cap. (The
   agent has one of its own, `SWH_GATEWAY_IMAGE`, a locally built image to run
   instead of the pinned one; it never reaches the gateway.) If you are typing a
   connection string, a port, or a credential into Go source, a test, or a
   manifest, you are encoding something true only on your machine for the next
   ten minutes. A test that needs a backend uses `mem`, which needs nothing, or
   `duckdb` in memory when it needs a real query engine — it runs in-process and
   needs no setup either.
5. **Diagnose, do not pattern-match.** "It started working when I set X" is
   not a diagnosis — set X back and confirm it breaks. Do not trust an error
   message before checking it. Measured here: `SWH_BACKEND=mem` used to fail
   with `SWH_DATABASE (or SWH_DSN) is required for backend "mem"`, and the
   message was wrong — `mem` never reads `Database`. Supplying a placeholder
   database would have "worked" and taught the wrong thing; the fix was the
   guard.
6. **Say what you did not verify.** Unverified is not the same as working. No
   cloud backend is exercised against its real warehouse from CI — there is no
   BigQuery, Snowflake, Redshift, or ClickHouse there. The `bigquery` backend is
   unit-tested against a fake of BigQuery's REST API
   (`internal/backend/bigquery/fake_test.go`) and, for `Scan`, a fake of the
   Storage Read API's gRPC service (`readfake_test.go`), which show what the
   backend sends and how it reads an answer, not what BigQuery does; its integration
   test runs only when `SWH_TEST_BIGQUERY_PROJECT` is set. A change touching a
   cloud path is therefore unproven against the real service unless that test
   was run, and the PR says which.

## Build and test

Derived from [`.github/workflows/ci.yml`](.github/workflows/ci.yml): the
`build-test` job is those three commands, so a green local run of all three is
the definition of done. CI pins the Go version as a literal in that workflow
(`go-version: "1.27.2"`) rather than reading `go.mod`, so bumping `go.mod`
alone moves your toolchain and not CI's — change both.

```sh
go build ./...
go vet ./...
go test -race ./...
```

Two more CI jobs need Docker and are not part of that gate. They build the image
(`docker build -t service-warehouse:e2e .`) and run the agent against it, behind
the `e2e` build tag:

```sh
SWH_GATEWAY_IMAGE=service-warehouse:e2e go test -v -tags e2e -count=1 -run TestRuntimeEndToEnd .
SWH_GATEWAY_IMAGE=service-warehouse:e2e go test -v -tags e2e -count=1 -run TestImageSBOM .   # needs syft on PATH
```

**A green e2e run is not evidence that anything ran.** With
`SWH_GATEWAY_IMAGE` unset both tests call `t.Skip`, the package still prints
`ok`, and `go test` exits 0. The SBOM one also skips without `syft`. Read `-v`
output for `--- PASS`, never `ok` alone.

`codefly agent ci` is the full agent conformance gate. Its workflow
(`agent-ci.yml`) is `workflow_dispatch` only until a full run has been seen green
here. Run `GOTOOLCHAIN=go1.27.2 codefly agent ci` from the repo root with CLI
0.1.185, also pinned in `agent-ci.yml`. The manifest
pins source packager `codefly.dev/go:0.0.69`, whose image recipes use immutable
digests. The gateway build pins Go 1.27.2 by tag and image-index digest and fixes
`GOTOOLCHAIN`; keep `go.mod`, CI and the image toolchain in sync. A fresh service
declares `backend: mem`; Runtime.Test probes health and authenticated Capabilities
even when Start was not called, and returns an explicit PASSED/FAILED run result.
Before the first published lock, set `SWH_GATEWAY_IMAGE` to a local build to
exercise the generated-service test phase; published-image audit still refuses
the absent digest. A passing binary audit is not full conformance. `codefly ci run` operates on a consuming workspace: this agent
repository has no workspace manifest.

Run the server locally — it needs no database, no warehouse credential, and no
network. It still refuses an unauthenticated listener, so a local run states
that choice (or sets `SWH_AUTH_TOKEN`):

```sh
SWH_ALLOW_ANONYMOUS=true go run ./cmd/service-warehouse                                  # the embedded DuckDB engine, in memory
SWH_ALLOW_ANONYMOUS=true SWH_DATABASE=./warehouse.duckdb go run ./cmd/service-warehouse  # the same, kept in a file
SWH_BACKEND=mem SWH_ALLOW_ANONYMOUS=true go run ./cmd/service-warehouse                  # catalog only, no query engine
```

DuckDB is cgo: building needs a C compiler, and on Linux a C++ runtime (`g++`
for `libstdc++`). `ubuntu-latest` has both, so CI needs no extra step; a
machine without them fails to link, which is not a bug in the repo. The engine
is opened closed on purpose (no host files, no extensions, locked
configuration — see the README), and it refuses to start when it was asked for
something it cannot honor, such as `SWH_MAX_QUERY_BYTES`. A hard failure at
startup is the intended behaviour, not a bug to route around.

**Regenerating stubs is not a gate, and that is a trap.** CI never runs `buf`,
so nothing catches generated-file drift. The committed stubs reproduce
byte-identically only under `protoc-gen-go v1.36.11` and
`protoc-gen-go-grpc v1.6.1`; a newer plugin rewrites the version banner and
nothing else, so an unrelated PR silently carries a `gen/` diff. Neither
version is pinned anywhere in the repo (codefly-dev/service-warehouse#7). If
`buf generate` leaves you with a diff that is only those banner lines, revert
it — you changed your toolchain, not the contract.

## Releases

A `v*` tag publishes the gateway image's tags first and the agent binary second,
from one workflow, in that order. The image itself is published *before* the
tag, because the agent pins its digest and a digest cannot be recorded in a
commit the tag was already cut from; `README.md` ("Releasing") has the nine
first-release steps. The manifest seed is 0.0.1; publish bumps the first release
to v0.0.2, so dispatch the image publisher with 0.0.2. `gateway-image.json` records no digest until the first image exists, and
every consumer of it — the agent, `Deploy`, the release — refuses an empty one
rather than run `:latest`. The tag must match `version:` in `agent.codefly.yaml`
or the job fails by design.

The coordinator must use an explicit squash body with `Refs #12`; commit
`632feea` contains a closing directive that must not enter the squash message.
Keep the issue open until the release has been verified.

Core is pinned in two places that move together: the `codefly-dev/core` version
in `go.mod` and the commit SHA of core's reusable workflows in `release.yml` and
`agent-ci.yml` (never `@main`: `release.yml` passes `secrets: inherit`). The image
publisher is an explicitly labelled stopgap pending
[core#770](https://github.com/codefly-dev/core/issues/770), the core 0.18.2 composite
action: its job must stay in this repository so GHCR accepts its own job token.
Do not restore the reusable publisher call; README owns the transition procedure.

## Where the depth is

Keep this file short; depth belongs in the file that owns the subject.

- [README.md](README.md) — the API table, the backend matrix, the full `SWH_*`
  env surface, the repo layout, and the distribution/SBOM status.
- `proto/codefly/warehouse/v0/warehouse.proto` — the contract of record.
- `internal/backend/backend.go` — the provider-agnostic interface every
  backend implements, and the portable type model.
- [README.md](README.md#the-duckdb-engine) — what the DuckDB engine honors, the
  type map, the Arrow framing and what is refused. `internal/arrowipc` owns that
  framing for every backend; a backend that produces or ingests Arrow uses it
  and does not re-derive it.
- [`cmd/service-warehouse/distribution_test.go`](cmd/service-warehouse/distribution_test.go)
  — gates the claim that this repo ships one image, the gateway, and that its
  SBOM evidence exists. It lists every path that builds or deploys an image and
  fails on one that is not accounted for, so a second image is the day more
  image SBOM coverage is owed. The gate is scoped to this repo only.
- A repeated multi-step procedure belongs in a `.claude/skills/<name>/SKILL.md`
  of its own, or in a nested `AGENTS.md` next to the code it describes —
  not in another paragraph here. There are none yet: this repo has three
  commands and no procedure long enough to earn one.
