// Command service-warehouse runs the data-warehouse gateway: it resolves its
// configuration from SWH_* environment variables, opens the configured backend,
// and serves the uniform Warehouse gRPC API. Every backend is compiled in and
// selected by config; the client speaks only the gRPC contract and never links
// a warehouse SDK.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/codefly-dev/core/callertoken"
	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	whv0 "github.com/codefly-dev/service-warehouse/gen/codefly/warehouse/v0"
	"github.com/codefly-dev/service-warehouse/internal/backend"
	"github.com/codefly-dev/service-warehouse/internal/config"
	"github.com/codefly-dev/service-warehouse/internal/server"

	// Backends register themselves via init(); importing them compiles each into
	// the single binary and makes its kind selectable by config.
	_ "github.com/codefly-dev/service-warehouse/internal/backend/bigquery"
	_ "github.com/codefly-dev/service-warehouse/internal/backend/duckdb"
	_ "github.com/codefly-dev/service-warehouse/internal/backend/mem"
)

// shutdownGrace bounds how long a graceful stop waits for in-flight RPCs (a
// long-running Query stream can otherwise keep GracefulStop blocked) before the
// server is forced down.
const shutdownGrace = 15 * time.Second

func main() {
	if err := run(); err != nil {
		log.Fatalf("service-warehouse: %v", err)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}

	be, err := backend.Open(context.Background(), cfg.Backend)
	if err != nil {
		return err
	}
	defer func() { _ = be.Close() }()

	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}

	grpcServer := newGRPCServer(be, cfg.AuthToken)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down")
		gracefulStop(grpcServer, shutdownGrace)
	}()

	// Config resolution refuses an empty token unless the operator accepted an
	// unauthenticated listener, so the empty case here is that choice. The token
	// itself is never logged.
	authMode := "none"
	if cfg.AuthToken != "" {
		authMode = "token"
	}
	log.Printf("warehouse gateway listening on %s (backend=%s database=%s auth=%s)",
		cfg.ListenAddr, cfg.Backend.Kind, cfg.Backend.Database, authMode)
	return grpcServer.Serve(lis)
}

// newGRPCServer assembles the gateway's gRPC server: the Warehouse service over
// be, the standard health service, and — when token is set — the interceptors
// that refuse any call that does not present it. The health service is exempt
// from them (only Check and Watch) so a readiness probe, which carries
// no token, can still reach it.
//
// The health service reports the process as serving and nothing more. It is
// liveness, not a verdict on the backend: this server does not probe its
// warehouse, so it claims nothing about whether one is reachable.
func newGRPCServer(be backend.Backend, token string) *grpc.Server {
	var options []grpc.ServerOption
	if token != "" {
		options = append(options,
			grpc.ChainUnaryInterceptor(callertoken.UnaryServerInterceptor(token)),
			grpc.ChainStreamInterceptor(callertoken.StreamServerInterceptor(token)),
		)
	}
	srv := grpc.NewServer(options...)
	healthv1.RegisterHealthServer(srv, grpchealth.NewServer())
	whv0.RegisterWarehouseServer(srv, server.New(be))
	return srv
}

// gracefulStop drains in-flight RPCs, but escalates to a hard Stop if they do
// not finish within grace, so a stuck stream can never make shutdown hang.
func gracefulStop(srv *grpc.Server, grace time.Duration) {
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		srv.Stop()
		<-done
	}
}
