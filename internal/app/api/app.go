package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aminkbi/taskforge"
	"github.com/aminkbi/taskforge/internal/app/api/dashboard"
	"github.com/aminkbi/taskforge/internal/config"
	"github.com/aminkbi/taskforge/internal/httpserver"
	"github.com/aminkbi/taskforge/internal/observability"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
)

type App struct {
	server         *httpserver.Server
	client         *redis.Client
	connectTimeout time.Duration
}

func New(cfg config.Config, logger *slog.Logger, metrics *observability.Metrics) (*App, error) {
	options, err := cfg.RedisOptions(nil, logger.With("component", "redis"))
	if err != nil {
		return nil, fmt.Errorf("configure Redis: %w", err)
	}
	client := taskforgeredis.NewClient(options)
	options.Client = client
	b := taskforgeredis.New(options)

	queues := make([]string, 0, len(cfg.Control.WorkerPools))
	for _, pool := range cfg.Control.WorkerPools {
		queues = append(queues, pool.Queue)
	}
	slices.Sort(queues)
	queues = slices.Compact(queues)
	_ = metrics.RegisterQueueMetricsCollector(b, queues)
	_ = metrics.RegisterFairnessMetricsCollector(b, queues)
	_ = metrics.RegisterDeadLetterMetricsCollector(b, queues)
	_ = metrics.RegisterAdmissionStatusCollector(b, queues)
	_ = metrics.RegisterDependencyBudgetCollector(b)

	server := httpserver.New(cfg.HTTPServerConfig(), logger.With("component", "httpserver"), metrics.Handler(), nil, func(mux *http.ServeMux) {
		mux.Handle("/", httpserver.ReadOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"service":"taskforge-api","status":"ok"}`))
		})))
		mux.Handle("/v1/admin/ping", httpserver.ReadOnly(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","time":"` + time.Now().UTC().Format(time.RFC3339Nano) + `"}`))
		})))
		mux.Handle("/v1/admin/admission", httpserver.ReadOnly(admissionHandler(b, queues)))
		mux.Handle("/v1/admin/adaptive", httpserver.ReadOnly(adaptiveHandler(b, b, cfg.Control.WorkerPools)))
		mux.Handle("/v1/admin/workers", httpserver.ReadOnly(workerLifecycleHandler(b)))
		mux.Handle("/v1/tasks/", httpserver.ReadOnly(taskLookupHandler(b)))

		// Operator dashboard: a static config builder + live ops view backed
		// by the /v1/admin endpoints above. Served from the embedded assets.
		mux.Handle("/dashboard/", httpserver.ReadOnly(http.StripPrefix("/dashboard/", dashboard.Handler())))
		mux.Handle("/dashboard", httpserver.ReadOnly(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/dashboard/", http.StatusMovedPermanently)
		})))
	})

	return &App{server: server, client: client, connectTimeout: cfg.RedisConnectTimeout}, nil
}

func taskLookupHandler(reader taskforge.StateStore) http.HandlerFunc {
	type responseBody struct {
		TaskID         string `json:"task_id"`
		Name           string `json:"name,omitempty"`
		Queue          string `json:"queue,omitempty"`
		State          string `json:"state"`
		ErrorPresent   bool   `json:"error_present,omitempty"`
		CreatedAt      string `json:"created_at,omitempty"`
		StartedAt      string `json:"started_at,omitempty"`
		CompletedAt    string `json:"completed_at,omitempty"`
		UpdatedAt      string `json:"updated_at"`
		DeliveryCount  int    `json:"delivery_count,omitempty"`
		LastDeliveryID string `json:"last_delivery_id,omitempty"`
		LastLeaseOwner string `json:"last_lease_owner,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		taskID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
		taskID = strings.Trim(taskID, "/")
		if taskID == "" || strings.Contains(taskID, "/") {
			writeAPIError(w, http.StatusBadRequest, "task id is required")
			return
		}

		record, err := reader.Get(r.Context(), taskID)
		if errors.Is(err, taskforge.ErrTaskNotFound) {
			writeAPIError(w, http.StatusNotFound, "task not found")
			return
		}
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal server error")
			return
		}

		response := responseBody{
			TaskID:         record.TaskID,
			Name:           record.Name,
			Queue:          record.Queue,
			State:          string(record.State),
			ErrorPresent:   record.LastError != "",
			CreatedAt:      formatOptionalTime(record.CreatedAt),
			StartedAt:      formatOptionalTime(record.StartedAt),
			CompletedAt:    formatOptionalTime(record.CompletedAt),
			UpdatedAt:      formatOptionalTime(record.UpdatedAt),
			DeliveryCount:  record.DeliveryCount,
			LastDeliveryID: record.LastDeliveryID,
			LastLeaseOwner: record.LastLeaseOwner,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func workerLifecycleHandler(provider observability.WorkerLifecycleProvider) http.HandlerFunc {
	type workerStatus struct {
		WorkerID            string  `json:"worker_id"`
		Pool                string  `json:"pool"`
		Queue               string  `json:"queue"`
		State               string  `json:"state"`
		Pending             float64 `json:"pending"`
		Running             float64 `json:"running"`
		DrainStartedAt      string  `json:"drain_started_at,omitempty"`
		DrainDeadline       string  `json:"drain_deadline,omitempty"`
		LastShutdownOutcome string  `json:"last_shutdown_outcome,omitempty"`
		AbandonedDeliveries float64 `json:"abandoned_deliveries"`
		DrainLeaseLosses    float64 `json:"drain_lease_losses"`
		UpdatedAt           string  `json:"updated_at"`
	}
	type responseBody struct {
		Workers []workerStatus `json:"workers"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		snapshots, err := provider.WorkerLifecycleSnapshots(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal server error")
			return
		}

		response := responseBody{Workers: make([]workerStatus, 0, len(snapshots))}
		for _, snapshot := range snapshots {
			item := workerStatus{
				WorkerID:            snapshot.WorkerID,
				Pool:                snapshot.Pool,
				Queue:               snapshot.Queue,
				State:               snapshot.State,
				Pending:             snapshot.Pending,
				Running:             snapshot.Running,
				LastShutdownOutcome: snapshot.LastShutdownOutcome,
				AbandonedDeliveries: snapshot.AbandonedDeliveries,
				DrainLeaseLosses:    snapshot.DrainLeaseLosses,
				UpdatedAt:           snapshot.UpdatedAt.UTC().Format(time.RFC3339Nano),
			}
			if !snapshot.DrainStartedAt.IsZero() {
				item.DrainStartedAt = snapshot.DrainStartedAt.UTC().Format(time.RFC3339Nano)
			}
			if !snapshot.DrainDeadline.IsZero() {
				item.DrainDeadline = snapshot.DrainDeadline.UTC().Format(time.RFC3339Nano)
			}
			response.Workers = append(response.Workers, item)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}
}

func adaptiveHandler(statusProvider observability.AdaptiveStatusProvider, budgetProvider observability.DependencyBudgetUsageProvider, pools []taskforge.WorkerPoolConfig) http.HandlerFunc {
	type poolStatus struct {
		Pool                  string             `json:"pool"`
		Queue                 string             `json:"queue"`
		AdaptiveEnabled       bool               `json:"adaptive_enabled"`
		ConfiguredConcurrency float64            `json:"configured_concurrency"`
		EffectiveConcurrency  float64            `json:"effective_concurrency"`
		MinConcurrency        float64            `json:"min_concurrency"`
		MaxConcurrency        float64            `json:"max_concurrency"`
		LastAdjustmentAction  string             `json:"last_adjustment_action,omitempty"`
		LastAdjustmentReason  string             `json:"last_adjustment_reason,omitempty"`
		LastAdjustedAt        string             `json:"last_adjusted_at,omitempty"`
		HealthyWindows        float64            `json:"healthy_windows"`
		Signals               map[string]float64 `json:"signals"`
	}
	type budgetStatus struct {
		Budget   string  `json:"budget"`
		Capacity float64 `json:"capacity"`
		InUse    float64 `json:"in_use"`
	}
	type responseBody struct {
		Pools   []poolStatus   `json:"pools"`
		Budgets []budgetStatus `json:"budgets"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		response := responseBody{
			Pools:   make([]poolStatus, 0, len(pools)),
			Budgets: make([]budgetStatus, 0),
		}

		for _, pool := range pools {
			snapshot, err := statusProvider.AdaptiveStatusSnapshot(r.Context(), pool.Name)
			if err != nil {
				writeAPIError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			lastAdjustedAt := ""
			if !snapshot.LastAdjustedAt.IsZero() {
				lastAdjustedAt = snapshot.LastAdjustedAt.UTC().Format(time.RFC3339Nano)
			}
			response.Pools = append(response.Pools, poolStatus{
				Pool:                  pool.Name,
				Queue:                 pool.Queue,
				AdaptiveEnabled:       snapshot.AdaptiveEnabled,
				ConfiguredConcurrency: snapshot.ConfiguredConcurrency,
				EffectiveConcurrency:  snapshot.EffectiveConcurrency,
				MinConcurrency:        snapshot.MinConcurrency,
				MaxConcurrency:        snapshot.MaxConcurrency,
				LastAdjustmentAction:  snapshot.LastAdjustmentAction,
				LastAdjustmentReason:  snapshot.LastAdjustmentReason,
				LastAdjustedAt:        lastAdjustedAt,
				HealthyWindows:        snapshot.HealthyWindows,
				Signals: map[string]float64{
					"avg_latency_seconds": snapshot.AvgLatencySeconds,
					"error_rate":          snapshot.ErrorRate,
					"budget_blocked":      snapshot.BudgetBlocked,
					"backlog":             snapshot.Backlog,
				},
			})
		}

		budgets, err := budgetProvider.DependencyBudgetUsageSnapshots(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		for _, budget := range budgets {
			response.Budgets = append(response.Budgets, budgetStatus{
				Budget:   budget.Budget,
				Capacity: budget.Capacity,
				InUse:    budget.InUse,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(response)
	}
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
				writeAPIError(w, http.StatusInternalServerError, "internal server error")
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

func writeAPIError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: message})
}

func (a *App) Run(ctx context.Context) error {
	defer a.client.Close()
	connectCtx, cancel := context.WithTimeout(ctx, a.connectTimeout)
	defer cancel()
	if err := taskforgeredis.ValidateClient(connectCtx, a.client); err != nil {
		return fmt.Errorf("validate Redis connection: %w", err)
	}
	a.server.SetReady(true)
	return a.server.Run(ctx)
}
