package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/i40/production-system/services/device-registry/internal/service"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	service *service.Service
	router  *mux.Router
	server  *http.Server
}

type ServerConfig struct {
	Service *service.Service
	Port    int
}

func NewServer(cfg ServerConfig) *Server {
	s := &Server{
		service: cfg.Service,
		router:  mux.NewRouter(),
	}

	s.setupRoutes()

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: s.router,
	}

	return s
}

func (s *Server) setupRoutes() {
	// Health checks
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
	s.router.HandleFunc("/ready", s.handleReady).Methods("GET")

	// Metrics
	s.router.Handle("/metrics", promhttp.Handler())

	// API routes
	api := s.router.PathPrefix("/api/v1").Subrouter()
	api.HandleFunc("/devices", s.handleRegisterDevice).Methods("POST")
	api.HandleFunc("/devices/{unitID}", s.handleGetDevice).Methods("GET")
	api.HandleFunc("/devices/{unitID}/genealogy", s.handleGetGenealogy).Methods("GET")
	api.HandleFunc("/devices/{unitID}/status", s.handleUpdateStatus).Methods("PATCH")
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}

func (s *Server) handleRegisterDevice(w http.ResponseWriter, r *http.Request) {
	var req service.DeviceRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	device, err := s.service.RegisterDevice(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(device)
}

func (s *Server) handleGetDevice(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitID := vars["unitID"]

	device, err := s.service.GetDevice(r.Context(), unitID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(device)
}

func (s *Server) handleGetGenealogy(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitID := vars["unitID"]

	genealogy, err := s.service.GetDeviceGenealogy(r.Context(), unitID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(genealogy)
}

func (s *Server) handleUpdateStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	unitID := vars["unitID"]

	var req struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.service.UpdateDeviceStatus(r.Context(), unitID, req.Status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
