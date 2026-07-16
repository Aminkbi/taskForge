package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandlerServesIndex(t *testing.T) {
	t.Parallel()

	handler := http.StripPrefix("/dashboard/", Handler())

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
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tc.path, nil))

			if recorder.Code != http.StatusOK {
				t.Fatalf("GET %s: status = %d, want 200", tc.path, recorder.Code)
			}
			if !strings.Contains(recorder.Body.String(), tc.wantBody) {
				t.Errorf("GET %s: body does not contain %q", tc.path, tc.wantBody)
			}
		})
	}
}
