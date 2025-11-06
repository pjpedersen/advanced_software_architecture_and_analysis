package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/go/genealogy-service/internal/service"
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
	// Health check endpoints
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/ready", s.handleReady)

	// API endpoints
	s.mux.HandleFunc("/api/v1/genealogy/recall/query", s.handleRecallQuery)
	s.mux.HandleFunc("/api/v1/genealogy/device/", s.handleGetGenealogy)
	s.mux.HandleFunc("/api/v1/genealogy/lot/", s.handleGetByLotID)
	s.mux.HandleFunc("/api/v1/genealogy/firmware/", s.handleGetByFirmware)
	s.mux.HandleFunc("/api/v1/genealogy/ota/lock", s.handleOTALock)
}

// ServeHTTP implements http.Handler
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

func (s *Server) handleRecallQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleRecallQuery")
	defer span.End()

	var req service.RecallQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	// Generate query ID if not provided
	if req.QueryID == "" {
		req.QueryID = uuid.New().String()
	}

	// Execute recall query
	response, err := s.service.QueryRecallDevices(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to query devices: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (s *Server) handleGetGenealogy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleGetGenealogy")
	defer span.End()

	// Extract unit ID from path: /api/v1/genealogy/device/{unitID}
	unitID := r.URL.Path[len("/api/v1/genealogy/device/"):]
	if unitID == "" {
		http.Error(w, "Unit ID is required", http.StatusBadRequest)
		return
	}

	genealogy, err := s.service.GetCompleteGenealogy(ctx, unitID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get genealogy: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(genealogy)
}

func (s *Server) handleGetByLotID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleGetByLotID")
	defer span.End()

	// Extract lot ID from path: /api/v1/genealogy/lot/{lotID}
	lotID := r.URL.Path[len("/api/v1/genealogy/lot/"):]
	if lotID == "" {
		http.Error(w, "Lot ID is required", http.StatusBadRequest)
		return
	}

	unitIDs, err := s.service.GetDevicesByLotID(ctx, lotID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get devices: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"lot_id":       lotID,
		"device_count": len(unitIDs),
		"unit_ids":     unitIDs,
	})
}

func (s *Server) handleGetByFirmware(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleGetByFirmware")
	defer span.End()

	// Extract firmware version from path: /api/v1/genealogy/firmware/{version}
	version := r.URL.Path[len("/api/v1/genealogy/firmware/"):]
	if version == "" {
		http.Error(w, "Firmware version is required", http.StatusBadRequest)
		return
	}

	unitIDs, err := s.service.GetDevicesByFirmwareVersion(ctx, version)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get devices: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"firmware_version": version,
		"device_count":     len(unitIDs),
		"unit_ids":         unitIDs,
	})
}

func (s *Server) handleOTALock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, span := s.tracer.Start(r.Context(), "api.handleOTALock")
	defer span.End()

	var req service.OTALockRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.UnitIDs) == 0 {
		http.Error(w, "At least one unit ID is required", http.StatusBadRequest)
		return
	}

	if req.LockReason == "" {
		http.Error(w, "Lock reason is required", http.StatusBadRequest)
		return
	}

	if err := s.service.LockDevicesForOTA(ctx, req); err != nil {
		http.Error(w, fmt.Sprintf("Failed to lock devices: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":       "locked",
		"device_count": len(req.UnitIDs),
		"lock_reason":  req.LockReason,
	})
}

// StartServer starts the HTTP server
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
