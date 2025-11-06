package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/i40/production-system/services/device-registry/internal/service"
	_ "github.com/lib/pq"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(databaseURL string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresStore{db: db}, nil
}

func (s *PostgresStore) CheckDeviceExists(ctx context.Context, pcbID, serialNumber string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM device_registry.devices WHERE pcb_id = $1 OR serial_number = $2)`

	err := s.db.QueryRowContext(ctx, query, pcbID, serialNumber).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (s *PostgresStore) GetDeviceByIdentifiers(ctx context.Context, pcbID, serialNumber string) (*service.Device, error) {
	query := `
		SELECT device_id, unit_id, serial_number, pcb_id, firmware_version, firmware_hash,
			   identity_type, certificate_pem, key_id, identity_expires_at,
			   registered_at, status
		FROM device_registry.devices
		WHERE pcb_id = $1 OR serial_number = $2
		ORDER BY registered_at DESC
		LIMIT 1
	`

	var device service.Device
	var certPEM sql.NullString

	err := s.db.QueryRowContext(ctx, query, pcbID, serialNumber).Scan(
		&device.DeviceID,
		&device.UnitID,
		&device.SerialNumber,
		&device.PCBID,
		&device.FirmwareVersion,
		&device.FirmwareHash,
		&device.Identity.IdentityType,
		&certPEM,
		&device.Identity.KeyID,
		&device.Identity.ExpiresAt,
		&device.RegisteredAt,
		&device.Status,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if certPEM.Valid {
		device.Identity.CertificatePEM = certPEM.String
	}

	return &device, nil
}

func (s *PostgresStore) SaveDeviceWithOutbox(
	ctx context.Context,
	device *service.Device,
	validations []service.ValidationResult,
	eventID uuid.UUID,
	eventBytes []byte,
) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Insert device
	deviceQuery := `
		INSERT INTO device_registry.devices (
			device_id, unit_id, serial_number, pcb_id,
			firmware_version, firmware_hash, firmware_artifact_id,
			identity_type, certificate_pem, key_id, identity_expires_at,
			registered_at, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`

	_, err = tx.ExecContext(ctx, deviceQuery,
		device.DeviceID,
		device.UnitID,
		device.SerialNumber,
		device.PCBID,
		device.FirmwareVersion,
		device.FirmwareHash,
		nil, // firmware_artifact_id
		device.Identity.IdentityType,
		device.Identity.CertificatePEM,
		device.Identity.KeyID,
		device.Identity.ExpiresAt,
		device.RegisteredAt,
		device.Status,
	)
	if err != nil {
		return fmt.Errorf("failed to insert device: %w", err)
	}

	// 2. Insert validation results
	for _, val := range validations {
		measurementsJSON, _ := json.Marshal(val.Measurements)

		valQuery := `
			INSERT INTO device_registry.validation_results (
				device_id, test_id, test_type, passed, measurements, tested_at
			) VALUES ((SELECT device_id FROM device_registry.devices WHERE unit_id = $1), $2, $3, $4, $5, $6)
		`

		_, err = tx.ExecContext(ctx, valQuery,
			device.UnitID,
			val.TestID,
			val.TestType,
			val.Passed,
			measurementsJSON,
			val.TestedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to insert validation result: %w", err)
		}
	}

	// 3. Insert genealogy
	genealogyQuery := `
		INSERT INTO genealogy.material_genealogy (
			device_id, pellet_lot_id, pellet_supplier, enclosure_color,
			pcb_batch_id, pcb_supplier
		) VALUES ((SELECT device_id FROM device_registry.devices WHERE unit_id = $1), $2, $3, $4, $5, $6)
	`

	_, err = tx.ExecContext(ctx, genealogyQuery,
		device.UnitID,
		device.Genealogy.PelletLotID,
		device.Genealogy.PelletSupplier,
		device.Genealogy.Color,
		device.Genealogy.PCBBatchID,
		device.Genealogy.PCBSupplier,
	)
	if err != nil {
		return fmt.Errorf("failed to insert genealogy: %w", err)
	}

	// 4. Insert outbox event
	outboxQuery := `
		INSERT INTO outbox.events (event_id, event_type, topic, payload, correlation_id)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err = tx.ExecContext(ctx, outboxQuery,
		eventID,
		"device.registered",
		"device.registered",
		eventBytes,
		device.DeviceID,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *PostgresStore) GetDevice(ctx context.Context, unitID string) (*service.Device, error) {
	query := `
		SELECT device_id, unit_id, serial_number, pcb_id, firmware_version, firmware_hash,
			   identity_type, certificate_pem, key_id, identity_expires_at,
			   registered_at, status
		FROM device_registry.devices
		WHERE unit_id = $1
	`

	var device service.Device
	var certPEM sql.NullString

	err := s.db.QueryRowContext(ctx, query, unitID).Scan(
		&device.DeviceID,
		&device.UnitID,
		&device.SerialNumber,
		&device.PCBID,
		&device.FirmwareVersion,
		&device.FirmwareHash,
		&device.Identity.IdentityType,
		&certPEM,
		&device.Identity.KeyID,
		&device.Identity.ExpiresAt,
		&device.RegisteredAt,
		&device.Status,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("device not found: %s", unitID)
	}
	if err != nil {
		return nil, err
	}

	if certPEM.Valid {
		device.Identity.CertificatePEM = certPEM.String
	}

	// Fetch genealogy
	genQuery := `
		SELECT pellet_lot_id, pellet_supplier, enclosure_color, pcb_batch_id, pcb_supplier
		FROM genealogy.material_genealogy
		WHERE device_id = $1
	`

	err = s.db.QueryRowContext(ctx, genQuery, device.DeviceID).Scan(
		&device.Genealogy.PelletLotID,
		&device.Genealogy.PelletSupplier,
		&device.Genealogy.Color,
		&device.Genealogy.PCBBatchID,
		&device.Genealogy.PCBSupplier,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to fetch genealogy: %w", err)
	}

	return &device, nil
}

func (s *PostgresStore) GetCompleteGenealogy(ctx context.Context, unitID string) (*service.CompleteGenealogy, error) {
	device, err := s.GetDevice(ctx, unitID)
	if err != nil {
		return nil, err
	}

	// Fetch validation results
	valQuery := `
		SELECT test_id, test_type, passed, measurements, tested_at
		FROM device_registry.validation_results
		WHERE device_id = $1
		ORDER BY tested_at
	`

	rows, err := s.db.QueryContext(ctx, valQuery, device.DeviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var validations []service.ValidationResult
	for rows.Next() {
		var val service.ValidationResult
		var measurementsJSON []byte

		err := rows.Scan(&val.TestID, &val.TestType, &val.Passed, &measurementsJSON, &val.TestedAt)
		if err != nil {
			return nil, err
		}

		if len(measurementsJSON) > 0 {
			json.Unmarshal(measurementsJSON, &val.Measurements)
		}

		validations = append(validations, val)
	}

	genealogy := &service.CompleteGenealogy{
		Device:            device,
		Materials:         device.Genealogy,
		ProcessSteps:      []service.ProcessStep{}, // Would fetch from process_genealogy table
		ValidationResults: validations,
	}

	return genealogy, nil
}

func (s *PostgresStore) UpdateDeviceStatus(ctx context.Context, unitID string, status string) error {
	query := `
		UPDATE device_registry.devices
		SET status = $1, updated_at = NOW()
		WHERE unit_id = $2
	`

	result, err := s.db.ExecContext(ctx, query, status, unitID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("device not found: %s", unitID)
	}

	return nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
