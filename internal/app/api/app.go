package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge/internal/brokerredis"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
)

type App struct {
	server *httpserver.Server
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) *App {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	fairnessPolicies := config.FairnessPoliciesByQueue(cfg.WorkerPools)
	admissionPolicies := admissionPoliciesByQueue(cfg.WorkerPools)
	b := brokerredis.NewWithOptions(client, logger.With("component", "brokerredis"), cfg.WorkerPools[0].LeaseTTL, metrics, brokerredis.Options{
		FairnessPolicies:  fairnessPolicies,
		AdmissionPolicies: admissionPolicies,
	})

	queues := make([]string, 0, len(cfg.WorkerPools))
	for _, pool := range cfg.WorkerPools {
		queues = append(queues, pool.Queue)
	}
	slices.Sort(queues)
	queues = slices.Compact(queues)
	_ = metrics.RegisterQueueMetricsCollector(b, queues)
	_ = metrics.RegisterFairnessMetricsCollector(b, queues)
	_ = metrics.RegisterDeadLetterMetricsCollector(b, queues)
	_ = metrics.RegisterAdmissionStatusCollector(b, queues)

	server := httpserver.New(cfg.HTTPAddr, logger.With("component", "httpserver"), metrics.Handler(), nil, func(mux *http.ServeMux) {
		mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"taskforge-api","status":"ok"}`))
		})
		mux.HandleFunc("/v1/admin/ping", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`))
		})
		mux.HandleFunc("/v1/admin/admission", admissionHandler(b, queues))
	})

	return &App{server: server}
}

func admissionPoliciesByQueue(pools []config.WorkerPoolConfig) map[string]brokerredis.AdmissionPolicy {
	policies := make(map[string]brokerredis.AdmissionPolicy)
	for queue, policy := range config.AdmissionPoliciesByQueue(pools) {
		policies[queue] = brokerredis.AdmissionPolicy{
			Mode:                     brokerredis.AdmissionMode(policy.Mode),
			MaxPending:               policy.MaxPending,
			MaxPendingPerFairnessKey: policy.MaxPendingPerFairnessKey,
			MaxOldestReadyAge:        policy.MaxOldestReadyAge,
			MaxRetryBacklog:          policy.MaxRetryBacklog,
			MaxDeadLetterSize:        policy.MaxDeadLetterSize,
			DeferInterval:            policy.DeferInterval,
		}
	}
	if len(policies) == 0 {
		return nil
	}
	return policies
}

func admissionHandler(provider observability.AdmissionStatusProvider, queues []string) http.HandlerFunc {
	type queueStatus struct {
		Queue         string             `json:"queue"`
		Mode          string             `json:"mode"`
		State         string             `json:"state"`
		Reason        string             `json:"reason,omitempty"`
		Signals       map[string]float64 `json:"signals"`
		DeferInterval string             `json:"defer_interval"`
		UpdatedAt     string             `json:"updated_at"`
	}
	type responseBody struct {
		Queues []queueStatus `json:"queues"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		now := time.Now().UTC()
		response := responseBody{Queues: make([]queueStatus, 0, len(queues))}
		for _, queue := range queues {
			snapshot, err := provider.AdmissionStatusSnapshot(r.Context(), queue, now)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			response.Queues = append(response.Queues, queueStatus{
				Queue:  queue,
				Mode:   snapshot.Mode,
				State:  snapshot.State,
				Reason: snapshot.Reason,
				Signals: map[string]float64{
					"queue_pending":         snapshot.QueuePending,
					"fairness_key_pending":  snapshot.FairnessKeyPending,
					"oldest_ready_age_secs": snapshot.OldestReadyAge,
					"retry_backlog":         snapshot.RetryBacklog,
					"dead_letter_size":      snapshot.DeadLetterSize,
				},
				DeferInterval: snapshot.DeferInterval.String(),
				UpdatedAt:     snapshot.UpdatedAt.UTC().Format(time.RFC3339Nano),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func (a *App) Run(ctx context.Context) error {
	a.server.SetReady(true)
	return a.server.Run(ctx)
}
