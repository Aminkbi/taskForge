package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
)

type statusResponse struct {
	Status string `json:"status"`
}

type readinessCheck struct {
	Status  string `json:"status"`
	Detail  string `json:"detail,omitempty"`
	Leader  bool   `json:"leader,omitempty"`
	Updated string `json:"updated_at,omitempty"`
}

type readinessResponse struct {
	Status string                    `json:"status"`
	Checks map[string]readinessCheck `json:"checks,omitempty"`
}

func healthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
	})
}

type ReadinessEvaluator interface {
	Evaluate(context.Context) readinessResponse
}

func readinessHandler(evaluator ReadinessEvaluator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := readinessResponse{Status: "ready"}
		if evaluator != nil {
			response = evaluator.Evaluate(r.Context())
		}

		statusCode := http.StatusOK
		if response.Status != "ready" {
			statusCode = http.StatusServiceUnavailable
		}
		writeJSON(w, statusCode, response)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
