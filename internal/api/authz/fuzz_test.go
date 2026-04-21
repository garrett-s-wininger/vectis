package authz

import (
	"net/http"
	"testing"
)

func FuzzActionForRequest(f *testing.F) {
	f.Add("GET", "/api/v1/jobs")
	f.Add("POST", "/api/v1/setup/complete")
	f.Add("", "")

	f.Fuzz(func(t *testing.T, method, path string) {
		req, err := http.NewRequest(method, path, nil)
		if err != nil {
			return
		}

		_ = ActionForRequest(req)
	})
}
