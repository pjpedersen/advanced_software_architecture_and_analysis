package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	pq "github.com/lib/pq"
)

// Store defines the interface for genealogy storage operations
type Store interface {
	QueryRecallDevices(ctx context.Context, criteria RecallCriteria) ([]RecallDevice, error)
	GetCompleteGenealogy(ctx context.Context, unitID string) (*CompleteGenealogy, error)
	GetDevicesByLotID(ctx context.Context, lotID string) ([]string, error)
	GetDevicesByFirmwareVersion(ctx context.Context, version string) ([]string, error)
	GetDevicesByDateRange(ctx context.Context, start, end time.Time) ([]string, error)
	Close() error
}

// RecallCriteria defines the criteria for recall queries
type RecallCriteria struct {
	PelletLotIDs      []string
	ColorIDs          []string
	FirmwareVersions  []string
	ProcessStationIDs []string
	StartDate         *time.Time
	EndDate           *time.Time
	ProductionBatch   string
}

// RecallDevice represents a device affected by a recall
type RecallDevice struct {
	UnitID         string                 `json:"unit_id"`
	SerialNumber   string                 `json:"serial_number"`
	ManufacturedAt time.Time              `json:"manufactured_at"`
	CustomerOrder  string                 `json:"customer_order,omitempty"`
	ShippedAt      *time.Time             `json:"shipped_at,omitempty"`
	Destination    string                 `json:"destination,omitempty"`
	Material       map[string]interface{} `json:"material"`
	Process        map[string]interface{} `json:"process"`
	Software       map[string]interface{} `json:"software"`
}

// CompleteGenealogy represents the full genealogy of a device
type CompleteGenealogy struct {
	DeviceID       string                 `json:"device_id"`
	UnitID         string                 `json:"unit_id"`
	SerialNumber   string                 `json:"serial_number"`
	PCBID          string                 `json:"pcb_id"`
	ManufacturedAt time.Time              `json:"manufactured_at"`
	Status         string                 `json:"status"`
	Material       map[string]interface{} `json:"material"`
	Process        map[string]interface{} `json:"process"`
	Software       map[string]interface{} `json:"software"`
	Shipping       map[string]interface{} `json:"shipping,omitempty"`
}

type postgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a new PostgreSQL store
func NewPostgresStore(databaseURL string) (Store, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &postgresStore{db: db}, nil
}

// QueryRecallDevices finds all devices matching the recall criteria
func (s *postgresStore) QueryRecallDevices(ctx context.Context, criteria RecallCriteria) ([]RecallDevice, error) {
	query := `
		SELECT unit_id, serial_number, registered_at
		FROM device_registry.devices d
		WHERE 1=1
	`

	args := []interface{}{}
	argCount := 1

	if len(criteria.FirmwareVersions) > 0 {
		query += fmt.Sprintf(" AND firmware_version = ANY($%d)", argCount)
		args = append(args, pq.Array(criteria.FirmwareVersions))
		argCount++
	}

	if len(criteria.PelletLotIDs) > 0 {
		query += fmt.Sprintf(" AND pcb_id = ANY($%d)", argCount)
		args = append(args, pq.Array(criteria.PelletLotIDs))
		argCount++
	}

	if criteria.StartDate != nil {
		query += fmt.Sprintf(" AND registered_at >= $%d", argCount)
		args = append(args, criteria.StartDate)
		argCount++
	}

	if criteria.EndDate != nil {
		query += fmt.Sprintf(" AND registered_at <= $%d", argCount)
		args = append(args, criteria.EndDate)
		argCount++
	}

	query += " ORDER BY registered_at DESC"

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query recall devices: %w", err)
	}
	defer rows.Close()

	var devices []RecallDevice
	for rows.Next() {
		var device RecallDevice
		if err := rows.Scan(&device.UnitID, &device.SerialNumber, &device.ManufacturedAt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		device.Material = map[string]interface{}{
			"pellet_lot_ids": criteria.PelletLotIDs,
			"color_ids":      criteria.ColorIDs,
		}
		device.Process = map[string]interface{}{
			"process_station_ids": criteria.ProcessStationIDs,
		}
		device.Software = map[string]interface{}{
			"firmware_versions": criteria.FirmwareVersions,
		}

		devices = append(devices, device)
	}

	return devices, rows.Err()
}

// GetCompleteGenealogy retrieves the complete genealogy for a device
func (s *postgresStore) GetCompleteGenealogy(ctx context.Context, unitID string) (*CompleteGenealogy, error) {
	query := `
		SELECT
			d.device_id,
			d.unit_id,
			d.serial_number,
			d.pcb_id,
			d.manufactured_at,
			d.status,
			mg.data AS material_data,
			pg.data AS process_data,
			sg.data AS software_data,
			s.data AS shipping_data
		FROM device_registry.devices d
		LEFT JOIN genealogy.material_genealogy mg ON d.device_id = mg.device_id
		LEFT JOIN genealogy.process_genealogy pg ON d.device_id = pg.device_id
		LEFT JOIN genealogy.software_genealogy sg ON d.device_id = sg.device_id
		LEFT JOIN genealogy.shipping_info s ON d.device_id = s.device_id
		WHERE d.unit_id = $1
	`

	var genealogy CompleteGenealogy
	var materialData, processData, softwareData, shippingData []byte

	err := s.db.QueryRowContext(ctx, query, unitID).Scan(
		&genealogy.DeviceID,
		&genealogy.UnitID,
		&genealogy.SerialNumber,
		&genealogy.PCBID,
		&genealogy.ManufacturedAt,
		&genealogy.Status,
		&materialData,
		&processData,
		&softwareData,
		&shippingData,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("device not found: %s", unitID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query genealogy: %w", err)
	}

	// Parse JSON data
	if len(materialData) > 0 {
		json.Unmarshal(materialData, &genealogy.Material)
	}
	if len(processData) > 0 {
		json.Unmarshal(processData, &genealogy.Process)
	}
	if len(softwareData) > 0 {
		json.Unmarshal(softwareData, &genealogy.Software)
	}
	if len(shippingData) > 0 {
		json.Unmarshal(shippingData, &genealogy.Shipping)
	}

	return &genealogy, nil
}

// GetDevicesByLotID finds all devices manufactured from a specific pellet lot
func (s *postgresStore) GetDevicesByLotID(ctx context.Context, lotID string) ([]string, error) {
	query := `
		SELECT DISTINCT d.unit_id
		FROM device_registry.devices d
		INNER JOIN genealogy.material_genealogy mg ON d.device_id = mg.device_id
		WHERE mg.pellet_lot_id = $1
		ORDER BY d.manufactured_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, lotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query by lot ID: %w", err)
	}
	defer rows.Close()

	var unitIDs []string
	for rows.Next() {
		var unitID string
		if err := rows.Scan(&unitID); err != nil {
			return nil, fmt.Errorf("failed to scan unit ID: %w", err)
		}
		unitIDs = append(unitIDs, unitID)
	}

	return unitIDs, rows.Err()
}

// GetDevicesByFirmwareVersion finds all devices with a specific firmware version
func (s *postgresStore) GetDevicesByFirmwareVersion(ctx context.Context, version string) ([]string, error) {
	query := `
		SELECT DISTINCT d.unit_id
		FROM device_registry.devices d
		INNER JOIN genealogy.software_genealogy sg ON d.device_id = sg.device_id
		WHERE sg.firmware_version = $1
		ORDER BY d.manufactured_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, version)
	if err != nil {
		return nil, fmt.Errorf("failed to query by firmware version: %w", err)
	}
	defer rows.Close()

	var unitIDs []string
	for rows.Next() {
		var unitID string
		if err := rows.Scan(&unitID); err != nil {
			return nil, fmt.Errorf("failed to scan unit ID: %w", err)
		}
		unitIDs = append(unitIDs, unitID)
	}

	return unitIDs, rows.Err()
}

// GetDevicesByDateRange finds all devices manufactured within a date range
func (s *postgresStore) GetDevicesByDateRange(ctx context.Context, start, end time.Time) ([]string, error) {
	query := `
		SELECT unit_id
		FROM device_registry.devices
		WHERE manufactured_at BETWEEN $1 AND $2
		ORDER BY manufactured_at DESC
	`

	rows, err := s.db.QueryContext(ctx, query, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query by date range: %w", err)
	}
	defer rows.Close()

	var unitIDs []string
	for rows.Next() {
		var unitID string
		if err := rows.Scan(&unitID); err != nil {
			return nil, fmt.Errorf("failed to scan unit ID: %w", err)
		}
		unitIDs = append(unitIDs, unitID)
	}

	return unitIDs, rows.Err()
}

// Close closes the database connection
func (s *postgresStore) Close() error {
	return s.db.Close()
}
