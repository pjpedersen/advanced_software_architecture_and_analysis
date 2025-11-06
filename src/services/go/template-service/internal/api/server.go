package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/i40/production-system/services/go/template-service/internal/service"
	"go.opentelemetry.io/otel/trace"
)

// Server handles HTTP API requests
type Server struct {
	service *service.Service
	tracer  trace.Tracer
	mux     *http.ServeMux
}

// NewServer creates a new API server
func NewServer(svc *service.Service, tracer trace.Tracer) *Server {
	s := &Server{
		service: svc,
		tracer:  tracer,
		mux:     http.NewServeMux(),
	}

	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)
	s.mux.HandleFunc("/api/v1/templates", s.handleTemplates)
	s.mux.HandleFunc("/api/v1/templates/", s.handleTemplate)
	s.mux.HandleFunc("/api/v1/templates/approve/", s.handleApproveTemplate)
	s.mux.HandleFunc("/api/v1/jobs/", s.handleJob)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *Server) handleTemplates(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "api.handleTemplates")
	defer span.End()

	switch r.Method {
	case http.MethodPost:
		s.handleCreateTemplate(ctx, w, r)
	case http.MethodGet:
		s.handleListTemplates(ctx, w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleCreateTemplate(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req service.CreateTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	template, err := s.service.CreateTemplate(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create template: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(template)
}

func (s *Server) handleListTemplates(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if offset < 0 {
		offset = 0
	}

	templates, err := s.service.ListTemplates(ctx, limit, offset)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list templates: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(templates)
}

func (s *Server) handleTemplate(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "api.handleTemplate")
	defer span.End()

	templateID := r.URL.Path[len("/api/v1/templates/"):]
	if templateID == "" {
		http.Error(w, "Template ID is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGetTemplate(ctx, w, r, templateID)
	case http.MethodPut:
		s.handleUpdateTemplate(ctx, w, r, templateID)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGetTemplate(ctx context.Context, w http.ResponseWriter, r *http.Request, templateID string) {
	template, err := s.service.GetTemplate(ctx, templateID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get template: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(template)
}

func (s *Server) handleUpdateTemplate(ctx context.Context, w http.ResponseWriter, r *http.Request, templateID string) {
	var req service.UpdateTemplateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	template, err := s.service.UpdateTemplate(ctx, templateID, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to update template: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(template)
}

func (s *Server) handleApproveTemplate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleApproveTemplate")
	defer span.End()

	templateID := r.URL.Path[len("/api/v1/templates/approve/"):]
	if templateID == "" {
		http.Error(w, "Template ID is required", http.StatusBadRequest)
		return
	}

	if err := s.service.ApproveTemplate(ctx, templateID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to approve template: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "approved"})
}

func (s *Server) handleJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleJob")
	defer span.End()

	jobID := r.URL.Path[len("/api/v1/jobs/"):]
	if jobID == "" {
		http.Error(w, "Job ID is required", http.StatusBadRequest)
		return
	}

	job, err := s.service.GetEngravingJob(ctx, jobID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get job: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func StartServer(ctx context.Context, addr string, handler http.Handler) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	errChan := make(chan error, 1)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
