package plugin

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func containsVersionValidityHeader(header http.Header) bool {
	v := header.Get(pluginVersion)
	return len(v) != 0
}

func TestGetTrace(t *testing.T) {
	s := httptest.NewServer()
}
