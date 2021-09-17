package plugin

import (
	"fmt"
	"net/http"
)

// wrapErr wraps error with proper text so that error in Jaeger UI and logs define
// which endpoint had thrown error.
func wrapErr(endpoint string, err error) error {
	return fmt.Errorf("promscale grpc-plugin component: %s; err: %w", endpoint, err)
}

// validateResponse returns an error if status code received is non 200.
func validateResponse(r *http.Response) error {
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("received status code %d, check Promscale for error logs", r.StatusCode)
	}
	return nil
}
