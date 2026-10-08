package server

import (
	"github.com/codefly-dev/service-warehouse/internal/serr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toStatus maps a normalized serr code to a gRPC status so clients branch on
// codes, never backend-specific SQLSTATE or reason strings.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	var c codes.Code
	switch serr.CodeOf(err) {
	case serr.NotFound:
		c = codes.NotFound
	case serr.AlreadyExists:
		c = codes.AlreadyExists
	case serr.PreconditionFailed:
		c = codes.FailedPrecondition
	case serr.Unsupported:
		c = codes.Unimplemented
	case serr.PermissionDenied:
		c = codes.PermissionDenied
	case serr.Throttled:
		c = codes.ResourceExhausted
	case serr.InvalidArgument:
		c = codes.InvalidArgument
	case serr.DeadlineExceeded:
		c = codes.DeadlineExceeded
	default:
		c = codes.Internal
	}
	return status.Error(c, err.Error())
}
