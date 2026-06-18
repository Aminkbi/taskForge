package dashboard

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesIndex(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.StripPrefix("/dashboard/", Handler()))
	defer srv.Close()

	cases := []struct {
		name     string
		path     string
		wantBody string
	}{
		{name: "index", path: "/dashboard/", wantBody: "TaskForge Dashboard"},
		{name: "stylesheet", path: "/dashboard/style.css", wantBody: "--accent"},
		{name: "script", path: "/dashboard/app.js", wantBody: "buildConfig"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(srv.URL + tc.path)
			if err != nil {
				t.Fatalf("GET %s: %v", tc.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s: status = %d, want 200", tc.path, resp.StatusCode)
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("GET %s: read body: %v", tc.path, err)
			}
			if !strings.Contains(string(body), tc.wantBody) {
				t.Errorf("GET %s: body does not contain %q", tc.path, tc.wantBody)
			}
		})
	}
}
