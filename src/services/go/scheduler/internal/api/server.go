package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/i40/production-system/services/scheduler/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// SchedulerService defines the contract required by the HTTP layer.
type SchedulerService interface {
	CreateProductionPlan(ctx context.Context, req scheduler.ProductionPlanRequest) (*scheduler.ProductionPlan, error)
	GetProductionPlan(ctx context.Context, planID string) (*scheduler.ProductionPlan, error)
}

// Server exposes REST endpoints for the scheduler.
type Server struct {
	service SchedulerService
	router  *mux.Router
	server  *http.Server
}

// ServerConfig describes the runtime configuration for the API server.
type ServerConfig struct {
	Service SchedulerService
	Port    int
}

var (
	metricsOnce sync.Once

	planCreatedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_plans_created_total",
		Help: "Number of production plans successfully created.",
	})
	planFailedCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "scheduler_plans_failed_total",
		Help: "Number of production plan creation attempts that failed.",
	})
	planCreationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "scheduler_plan_creation_duration_seconds",
		Help:    "Latency for creating production plans.",
		Buckets: prometheus.DefBuckets,
	})
)

// NewServer constructs a scheduler API server with the required routes.
func NewServer(cfg ServerConfig) *Server {
	registerMetrics()

	router := mux.NewRouter()

	s := &Server{
		service: cfg.Service,
		router:  router,
		server: &http.Server{
			Addr:    fmt.Sprintf(":%d", cfg.Port),
			Handler: router,
		},
	}

	api := router.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/plans", s.handleCreatePlan).Methods(http.MethodPost)
	api.HandleFunc("/plans/{plan_id}", s.handleGetPlan).Methods(http.MethodGet)

	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy"})
	})
	router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	router.Handle("/metrics", promhttp.Handler())

	return s
}

// Start begins serving HTTP traffic.
func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *Server) handleCreatePlan(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req createPlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		planFailedCounter.Inc()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request payload"})
		return
	}

	serviceReq, err := req.toServiceRequest()
	if err != nil {
		planFailedCounter.Inc()
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	plan, err := s.service.CreateProductionPlan(r.Context(), serviceReq)
	if err != nil {
		planFailedCounter.Inc()
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	planCreationDuration.Observe(time.Since(start).Seconds())
	planCreatedCounter.Inc()

	writeJSON(w, http.StatusCreated, plan)
}

func (s *Server) handleGetPlan(w http.ResponseWriter, r *http.Request) {
	planID := mux.Vars(r)["plan_id"]
	if planID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "plan_id is required"})
		return
	}

	plan, err := s.service.GetProductionPlan(r.Context(), planID)
	if err != nil {
		if errors.Is(err, scheduler.ErrPlanNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "plan not found"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, plan)
}

func registerMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(planCreatedCounter, planFailedCounter, planCreationDuration)
	})
}

func writeJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type createPlanRequest struct {
	PlanStart        string               `json:"start_time"`
	PlanEnd          string               `json:"end_time"`
	Batches          []createBatchRequest `json:"batches"`
	ChangeoverWindow []changeoverRequest  `json:"changeover_windows"`
}

type createBatchRequest struct {
	BatchID           string `json:"batch_id"`
	ProductSKU        string `json:"product_sku"`
	Quantity          int    `json:"quantity"`
	Color             string `json:"color"`
	EngravingTemplate string `json:"engraving_template_id"`
	Start             string `json:"start_time"`
	End               string `json:"end_time"`
	Priority          int    `json:"priority"`
}

type changeoverRequest struct {
	ChangeoverID             string `json:"changeover_id"`
	FromColor                string `json:"from_color"`
	ToColor                  string `json:"to_color"`
	ScheduledTime            string `json:"scheduled_time"`
	EstimatedDurationMinutes int    `json:"estimated_duration_minutes"`
	RequiresOperator         bool   `json:"requires_operator_confirmation"`
}

func (r createPlanRequest) toServiceRequest() (scheduler.ProductionPlanRequest, error) {
	if len(r.Batches) == 0 {
		return scheduler.ProductionPlanRequest{}, errors.New("at least one batch is required")
	}

	var (
		batches            []scheduler.BatchJob
		planStart, planEnd time.Time
	)

	for i, batch := range r.Batches {
		startTime, err := parseRFC3339(batch.Start, fmt.Sprintf("batches[%d].start_time", i))
		if err != nil {
			return scheduler.ProductionPlanRequest{}, err
		}

		endTime, err := parseRFC3339(batch.End, fmt.Sprintf("batches[%d].end_time", i))
		if err != nil {
			return scheduler.ProductionPlanRequest{}, err
		}

		if !planStart.IsZero() && startTime.Before(planStart) {
			planStart = startTime
		}
		if planStart.IsZero() {
			planStart = startTime
		}

		if endTime.After(planEnd) {
			planEnd = endTime
		}

		if batch.Quantity <= 0 {
			return scheduler.ProductionPlanRequest{}, fmt.Errorf("batches[%d].quantity must be positive", i)
		}

		batches = append(batches, scheduler.BatchJob{
			BatchID:           batch.BatchID,
			ProductSKU:        batch.ProductSKU,
			Quantity:          batch.Quantity,
			Color:             batch.Color,
			EngravingTemplate: batch.EngravingTemplate,
			ScheduledStart:    startTime,
			EstimatedDuration: endTime.Sub(startTime),
			Priority:          batch.Priority,
		})
	}

	if r.PlanStart != "" {
		startTime, err := parseRFC3339(r.PlanStart, "start_time")
		if err != nil {
			return scheduler.ProductionPlanRequest{}, err
		}
		planStart = startTime
	}

	if r.PlanEnd != "" {
		endTime, err := parseRFC3339(r.PlanEnd, "end_time")
		if err != nil {
			return scheduler.ProductionPlanRequest{}, err
		}
		planEnd = endTime
	}

	if planEnd.Before(planStart) {
		return scheduler.ProductionPlanRequest{}, errors.New("plan end_time cannot be before start_time")
	}

	changeovers := make([]scheduler.ChangeoverWindow, 0, len(r.ChangeoverWindow))
	for i, window := range r.ChangeoverWindow {
		scheduledTime, err := parseRFC3339(window.ScheduledTime, fmt.Sprintf("changeover_windows[%d].scheduled_time", i))
		if err != nil {
			return scheduler.ProductionPlanRequest{}, err
		}

		duration := time.Duration(window.EstimatedDurationMinutes) * time.Minute
		if duration <= 0 {
			return scheduler.ProductionPlanRequest{}, fmt.Errorf("changeover_windows[%d].estimated_duration_minutes must be positive", i)
		}

		changeovers = append(changeovers, scheduler.ChangeoverWindow{
			ChangeoverID:                 window.ChangeoverID,
			FromColor:                    window.FromColor,
			ToColor:                      window.ToColor,
			ScheduledTime:                scheduledTime,
			EstimatedDuration:            duration,
			RequiresOperatorConfirmation: window.RequiresOperator,
		})
	}

	return scheduler.ProductionPlanRequest{
		Batches:           batches,
		ChangeoverWindows: changeovers,
		StartTime:         planStart,
		EndTime:           planEnd,
	}, nil
}

func parseRFC3339(value string, field string) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("%s is required", field)
	}

	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s: %w", field, err)
	}
	return t, nil
}
