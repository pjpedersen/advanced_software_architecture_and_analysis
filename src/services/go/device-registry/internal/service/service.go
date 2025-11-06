package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	sharedkafka "github.com/i40/production-system/services/go/pkg/kafka"
	"github.com/prometheus/client_golang/prometheus"
)

type Service struct {
	store    Store
	producer *sharedkafka.Producer
}

var (
	deviceRegistrationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "device_registrations_total",
		Help: "Total number of devices registered successfully.",
	})
	deviceRegistrationDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "device_registration_duration_seconds",
		Help:    "Time spent processing device registrations.",
		Buckets: prometheus.DefBuckets,
	})
)

func init() {
	prometheus.MustRegister(deviceRegistrationsTotal, deviceRegistrationDuration)
}

type ServiceConfig struct {
	Store    Store
	Producer *sharedkafka.Producer
}

func NewService(cfg ServiceConfig) *Service {
	return &Service{
		store:    cfg.Store,
		producer: cfg.Producer,
	}
}

// DeviceRegistrationRequest represents a device registration request (UC-2)
type DeviceRegistrationRequest struct {
	PCBID             string             `json:"pcb_id"`
	SerialNumber      string             `json:"serial_number"`
	FirmwareVersion   string             `json:"firmware_version"`
	FirmwareHash      string             `json:"firmware_hash"`
	Genealogy         GenealogyData      `json:"genealogy"`
	ValidationResults []ValidationResult `json:"validation_results"`
}

type GenealogyData struct {
	PelletLotID     string `json:"pellet_lot_id"`
	PelletSupplier  string `json:"pellet_supplier"`
	Color           string `json:"color"`
	PCBBatchID      string `json:"pcb_batch_id"`
	PCBSupplier     string `json:"pcb_supplier"`
	EnclosureSerial string `json:"enclosure_serial"`
}

type ValidationResult struct {
	TestID       string             `json:"test_id"`
	TestType     string             `json:"test_type"`
	Passed       bool               `json:"passed"`
	Measurements map[string]float64 `json:"measurements"`
	TestedAt     time.Time          `json:"tested_at"`
}

// Device represents a registered device
type Device struct {
	DeviceID        string         `json:"device_id"`
	UnitID          string         `json:"unit_id"`
	SerialNumber    string         `json:"serial_number"`
	PCBID           string         `json:"pcb_id"`
	FirmwareVersion string         `json:"firmware_version"`
	FirmwareHash    string         `json:"firmware_hash"`
	Identity        DeviceIdentity `json:"identity"`
	Genealogy       GenealogyData  `json:"genealogy"`
	Status          string         `json:"status"`
	RegisteredAt    time.Time      `json:"registered_at"`
}

type DeviceIdentity struct {
	IdentityType   string    `json:"identity_type"`
	CertificatePEM string    `json:"certificate_pem,omitempty"`
	KeyID          string    `json:"key_id"`
	ExpiresAt      time.Time `json:"expires_at"`
}

// RegisterDevice registers a new device and issues identity (UC-2)
func (s *Service) RegisterDevice(ctx context.Context, req DeviceRegistrationRequest) (*Device, error) {
	start := time.Now()
	defer func() {
		deviceRegistrationDuration.Observe(time.Since(start).Seconds())
	}()

	// Generate unique device ID
	deviceID := uuid.New().String()
	unitID := s.generateUnitID(req.SerialNumber)

	// Check for duplicates
	exists, err := s.store.CheckDeviceExists(ctx, req.PCBID, req.SerialNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to check device existence: %w", err)
	}
	if exists {
		existing, err := s.store.GetDeviceByIdentifiers(ctx, req.PCBID, req.SerialNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch existing device: %w", err)
		}
		if existing != nil {
			log.Printf("Device already registered (PCB %s), returning existing record", req.PCBID)
			return existing, nil
		}
	}

	// Issue device identity (simplified - in production, call Vault)
	identity := s.issueDeviceIdentity(deviceID, req.SerialNumber)

	// Create device record
	device := &Device{
		DeviceID:        deviceID,
		UnitID:          unitID,
		SerialNumber:    req.SerialNumber,
		PCBID:           req.PCBID,
		FirmwareVersion: req.FirmwareVersion,
		FirmwareHash:    req.FirmwareHash,
		Identity:        identity,
		Genealogy:       req.Genealogy,
		Status:          "ACTIVE",
		RegisteredAt:    time.Now(),
	}

	// Save to database with outbox pattern
	eventID := uuid.New()
	event := DeviceRegisteredEvent{
		Metadata: EventMetadata{
			EventID:       eventID.String(),
			EventType:     "device.registered",
			Timestamp:     time.Now(),
			SourceService: "device-registry",
			SchemaVersion: 1,
		},
		DeviceID:          device.DeviceID,
		UnitID:            device.UnitID,
		SerialNumber:      device.SerialNumber,
		PCBID:             device.PCBID,
		FirmwareVersion:   device.FirmwareVersion,
		FirmwareHash:      device.FirmwareHash,
		Identity:          identity,
		Genealogy:         req.Genealogy,
		ValidationResults: req.ValidationResults,
		RegisteredAt:      device.RegisteredAt,
	}

	eventBytes, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Transactional save: device record + outbox event
	if err := s.store.SaveDeviceWithOutbox(ctx, device, req.ValidationResults, eventID, eventBytes); err != nil {
		return nil, fmt.Errorf("failed to save device: %w", err)
	}

	deviceRegistrationsTotal.Inc()
	log.Printf("Device %s registered successfully (Unit ID: %s)", device.DeviceID, device.UnitID)
	return device, nil
}

// GetDevice retrieves device by unit ID
func (s *Service) GetDevice(ctx context.Context, unitID string) (*Device, error) {
	device, err := s.store.GetDevice(ctx, unitID)
	if err != nil {
		return nil, fmt.Errorf("failed to get device: %w", err)
	}
	return device, nil
}

// GetDeviceGenealogy retrieves complete genealogy for a device
func (s *Service) GetDeviceGenealogy(ctx context.Context, unitID string) (*CompleteGenealogy, error) {
	genealogy, err := s.store.GetCompleteGenealogy(ctx, unitID)
	if err != nil {
		return nil, fmt.Errorf("failed to get genealogy: %w", err)
	}
	return genealogy, nil
}

// UpdateDeviceStatus updates device status (e.g., for recalls)
func (s *Service) UpdateDeviceStatus(ctx context.Context, unitID string, status string) error {
	if err := s.store.UpdateDeviceStatus(ctx, unitID, status); err != nil {
		return fmt.Errorf("failed to update device status: %w", err)
	}

	log.Printf("Device %s status updated to %s", unitID, status)
	return nil
}

// Helper functions

func (s *Service) generateUnitID(serialNumber string) string {
	// Generate unique unit ID from serial number + timestamp
	hash := sha256.Sum256([]byte(serialNumber + time.Now().String()))
	return "WIC-" + hex.EncodeToString(hash[:8])
}

func (s *Service) issueDeviceIdentity(deviceID, serialNumber string) DeviceIdentity {
	// Simplified identity issuance
	// In production, this would call Vault to generate X.509 cert or symmetric key
	keyID := fmt.Sprintf("key-%s", deviceID[:8])

	return DeviceIdentity{
		IdentityType: "X509_CERT",
		KeyID:        keyID,
		ExpiresAt:    time.Now().Add(365 * 24 * time.Hour), // 1 year
		// CertificatePEM would be generated by Vault
	}
}

// Event types
type EventMetadata struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	Timestamp     time.Time `json:"timestamp"`
	SourceService string    `json:"source_service"`
	SchemaVersion int       `json:"schema_version"`
}

type DeviceRegisteredEvent struct {
	Metadata          EventMetadata      `json:"metadata"`
	DeviceID          string             `json:"device_id"`
	UnitID            string             `json:"unit_id"`
	SerialNumber      string             `json:"serial_number"`
	PCBID             string             `json:"pcb_id"`
	FirmwareVersion   string             `json:"firmware_version"`
	FirmwareHash      string             `json:"firmware_hash"`
	Identity          DeviceIdentity     `json:"identity"`
	Genealogy         GenealogyData      `json:"genealogy"`
	ValidationResults []ValidationResult `json:"validation_results"`
	RegisteredAt      time.Time          `json:"registered_at"`
}

// CompleteGenealogy represents full traceability data
type CompleteGenealogy struct {
	Device            *Device            `json:"device"`
	Materials         GenealogyData      `json:"materials"`
	ProcessSteps      []ProcessStep      `json:"process_steps"`
	ValidationResults []ValidationResult `json:"validation_results"`
	ShippingInfo      *ShippingInfo      `json:"shipping_info,omitempty"`
}

type ProcessStep struct {
	StepID      string            `json:"step_id"`
	StationID   string            `json:"station_id"`
	Operation   string            `json:"operation"`
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Successful  bool              `json:"successful"`
	Parameters  map[string]string `json:"parameters"`
}

type ShippingInfo struct {
	ShipmentID      string    `json:"shipment_id"`
	CustomerOrderID string    `json:"customer_order_id"`
	CustomerID      string    `json:"customer_id"`
	ShippedAt       time.Time `json:"shipped_at"`
	Destination     string    `json:"destination"`
}

// Store interface
type Store interface {
	CheckDeviceExists(ctx context.Context, pcbID, serialNumber string) (bool, error)
	SaveDeviceWithOutbox(ctx context.Context, device *Device, validations []ValidationResult, eventID uuid.UUID, eventBytes []byte) error
	GetDevice(ctx context.Context, unitID string) (*Device, error)
	GetCompleteGenealogy(ctx context.Context, unitID string) (*CompleteGenealogy, error)
	UpdateDeviceStatus(ctx context.Context, unitID string, status string) error
	GetDeviceByIdentifiers(ctx context.Context, pcbID, serialNumber string) (*Device, error)
}
