package server

import (
	"errors"
	"testing"

	"github.com/codefly-dev/service-warehouse/internal/serr"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every normalized code reaches the client as one gRPC code, and only that code
// is what a client branches on.
func TestToStatusMapsEveryNormalizedCode(t *testing.T) {
	for code, want := range map[serr.Code]codes.Code{
		serr.Internal:           codes.Internal,
		serr.NotFound:           codes.NotFound,
		serr.AlreadyExists:      codes.AlreadyExists,
		serr.PreconditionFailed: codes.FailedPrecondition,
		serr.Unsupported:        codes.Unimplemented,
		serr.PermissionDenied:   codes.PermissionDenied,
		serr.Throttled:          codes.ResourceExhausted,
		serr.InvalidArgument:    codes.InvalidArgument,
		serr.DeadlineExceeded:   codes.DeadlineExceeded,
	} {
		require.Equal(t, want, status.Code(toStatus(serr.New(code, "Op", "msg"))), code.String())
	}
	// An error that carries no code is an internal failure, never a guess.
	require.Equal(t, codes.Internal, status.Code(toStatus(errors.New("plain"))))
	require.NoError(t, toStatus(nil))
}
