# The warehouse gateway image. A client speaks only the codefly/warehouse/v0
# gRPC contract; every backend is compiled in and selected at runtime by
# SWH_BACKEND. The image ships no warehouse credentials: those arrive as
# environment at deploy time.
#
# cgo is on, and the runtime image carries glibc and libstdc++ (distroless/cc).
# The embedded DuckDB engine is a cgo driver (internal/backend/duckdb), so a
# CGO_ENABLED=0 build onto distroless/static would keep building until the
# driver lands and then stop. Both stages name Debian 13 explicitly: the binary
# links against the build stage's glibc and needs one at least as new in the
# image it runs in, which "golang:1.27" moving to the next Debian would break.
FROM golang:1.27.3-trixie@sha256:1896654cfc5c2b5ad74c07f29cb36f5d2d1cbed42e3054070fbfe444e521e83c AS build
ENV GOTOOLCHAIN=go1.27.3
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/service-warehouse ./cmd/service-warehouse

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /out/service-warehouse /service-warehouse
# The gRPC listen port; SWH_LISTEN can override it.
EXPOSE 9465
ENTRYPOINT ["/service-warehouse"]
