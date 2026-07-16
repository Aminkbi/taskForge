package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testAuthToken = "0123456789abcdef0123456789abcdef"

func TestPublicReadinessRequiresStartupWithoutAuthentication(t *testing.T) {
	t.Parallel()

	server := New(Config{Addr: ":0"}, nil, http.NotFoundHandler(), nil, nil)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	if recorder.Body.String() != "{\"status\":\"not_ready\"}\n" {
		t.Fatalf("body = %q, want minimal not_ready status", recorder.Body.String())
	}
}

func TestPublicReadinessRedactsCheckDetails(t *testing.T) {
	t.Parallel()

	server := New(Config{Addr: ":0"}, nil, http.NotFoundHandler(), map[string]CheckFunc{
		"redis": func(context.Context) CheckResult {
			return CheckResult{Ready: true, Status: "ready", Detail: "redis.internal:6379 secret detail"}
		},
	}, nil)
	server.SetReady(true)

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Body.String() != "{\"status\":\"ready\"}\n" {
		t.Fatalf("body = %q, want minimal ready status", recorder.Body.String())
	}
}

func TestEvaluateIncludesStandbySchedulerStateForInternalCallers(t *testing.T) {
	t.Parallel()

	server := New(Config{Addr: ":0"}, nil, http.NotFoundHandler(), map[string]CheckFunc{
		"scheduler_leadership": func(context.Context) CheckResult {
			return CheckResult{
				Ready:     true,
				Status:    "ready",
				Detail:    "standby",
				Leader:    false,
				UpdatedAt: time.Date(2026, 4, 14, 10, 0, 0, 0, time.UTC),
			}
		},
	}, nil)
	server.SetReady(true)

	payload := server.Evaluate(context.Background())
	check, ok := payload.Checks["scheduler_leadership"]
	if !ok {
		t.Fatalf("scheduler_leadership check missing: %+v", payload.Checks)
	}
	if payload.Status != "ready" || check.Detail != "standby" || check.Leader {
		t.Fatalf("readiness payload = %+v, want ready standby non-leader", payload)
	}
}

func TestOperatorSurfaceDisabledByDefaultWhileHealthRemainsPublic(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0"})
	for _, path := range []string{"/metrics", "/operator"} {
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want %d", path, recorder.Code, http.StatusNotFound)
		}
	}

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /healthz: status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if recorder.Header().Get("X-Content-Type-Options") != "nosniff" || recorder.Header().Get("Content-Security-Policy") == "" {
		t.Fatalf("secure headers missing: %+v", recorder.Header())
	}
}

func TestOperatorAuthenticationAcceptsBearerAndBasicCredentials(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0", AuthToken: testAuthToken})
	tests := []struct {
		name      string
		authorize func(*http.Request)
		want      int
	}{
		{name: "missing", authorize: func(*http.Request) {}, want: http.StatusUnauthorized},
		{name: "wrong bearer", authorize: func(r *http.Request) { r.Header.Set("Authorization", "Bearer wrong") }, want: http.StatusUnauthorized},
		{name: "bearer", authorize: func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+testAuthToken) }, want: http.StatusOK},
		{name: "basic", authorize: func(r *http.Request) { r.SetBasicAuth("taskforge", testAuthToken) }, want: http.StatusOK},
		{name: "wrong basic user", authorize: func(r *http.Request) { r.SetBasicAuth("admin", testAuthToken) }, want: http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/operator", nil)
			tc.authorize(request)
			server.ServeHTTP(recorder, request)
			if recorder.Code != tc.want {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, tc.want, recorder.Body.String())
			}
		})
	}
}

func TestReadOnlyRoutesRejectOtherMethods(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0", AuthToken: testAuthToken})
	for _, path := range []string{"/healthz", "/operator"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.Header.Set("Authorization", "Bearer "+testAuthToken)
		server.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s: status = %d, want %d", path, recorder.Code, http.StatusMethodNotAllowed)
		}
		if recorder.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("POST %s: Allow = %q", path, recorder.Header().Get("Allow"))
		}
	}
}

func TestMutatingOperatorRouteRequiresBearerCredential(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0", AuthToken: testAuthToken})

	basicRequest := httptest.NewRequest(http.MethodPost, "/mutate", nil)
	basicRequest.SetBasicAuth("taskforge", testAuthToken)
	basicRecorder := httptest.NewRecorder()
	server.ServeHTTP(basicRecorder, basicRequest)
	if basicRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("Basic POST status = %d, want %d", basicRecorder.Code, http.StatusUnauthorized)
	}

	bearerRequest := httptest.NewRequest(http.MethodPost, "/mutate", nil)
	bearerRequest.Header.Set("Authorization", "Bearer "+testAuthToken)
	bearerRecorder := httptest.NewRecorder()
	server.ServeHTTP(bearerRecorder, bearerRequest)
	if bearerRecorder.Code != http.StatusNoContent {
		t.Fatalf("Bearer POST status = %d, want %d", bearerRecorder.Code, http.StatusNoContent)
	}
}

func TestServerRejectsOversizedRequestBody(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0", MaxBodyBytes: 4})
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", strings.NewReader("12345")))

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusRequestEntityTooLarge)
	}
	var payload map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if payload["error"] != "request body too large" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestServerRejectsChunkedRequestBody(t *testing.T) {
	t.Parallel()

	server := newTestServer(Config{Addr: ":0"})
	request := httptest.NewRequest(http.MethodGet, "/healthz", strings.NewReader("body"))
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusLengthRequired {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusLengthRequired)
	}
}

func TestServerAppliesExplicitConnectionAndShutdownLimits(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Addr:              ":0",
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       2 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       4 * time.Second,
		ShutdownTimeout:   5 * time.Second,
		MaxHeaderBytes:    8192,
	}
	server := newTestServer(cfg)

	if server.server.ReadHeaderTimeout != time.Second || server.server.ReadTimeout != 2*time.Second || server.server.WriteTimeout != 3*time.Second || server.server.IdleTimeout != 4*time.Second {
		t.Fatalf("server timeouts = %+v", server.server)
	}
	if server.shutdownTimeout != 5*time.Second || server.server.MaxHeaderBytes != 8192 {
		t.Fatalf("shutdown/header limits = %v/%d", server.shutdownTimeout, server.server.MaxHeaderBytes)
	}
}

func newTestServer(cfg Config) *Server {
	return New(cfg, nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), nil, func(mux *http.ServeMux) {
		mux.Handle("/operator", ReadOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})))
		mux.HandleFunc("/mutate", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
	})
}
