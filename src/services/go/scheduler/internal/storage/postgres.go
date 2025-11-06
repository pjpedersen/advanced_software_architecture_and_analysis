package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq" // postgres driver

	schedulersvc "github.com/i40/production-system/services/scheduler/internal/scheduler"
)

// PostgresStore persists scheduler state in PostgreSQL.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore creates a PostgreSQL-backed scheduler store.
func NewPostgresStore(conn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	store := &PostgresStore{db: db}
	if err := store.ensureSchema(); err != nil {
		db.Close()
		return nil, err
	}

	return store, nil
}

// Close releases the database connection.
func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) ensureSchema() error {
	statements := []string{
		`CREATE SCHEMA IF NOT EXISTS scheduler`,
		`CREATE TABLE IF NOT EXISTS scheduler.production_plans (
			plan_id TEXT PRIMARY KEY,
			batches JSONB NOT NULL,
			changeover_windows JSONB,
			start_time TIMESTAMPTZ NOT NULL,
			end_time TIMESTAMPTZ NOT NULL,
			resource_allocations JSONB NOT NULL,
			status TEXT NOT NULL,
			expected_adapters TEXT[] NOT NULL,
			created_at TIMESTAMPTZ NOT NULL,
			updated_at TIMESTAMPTZ NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS scheduler.plan_acks (
			plan_id TEXT NOT NULL REFERENCES scheduler.production_plans(plan_id) ON DELETE CASCADE,
			adapter_id TEXT NOT NULL,
			result TEXT NOT NULL,
			ack_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (plan_id, adapter_id)
		)`,
	}

	for _, stmt := range statements {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to ensure scheduler schema: %w", err)
		}
	}

	return nil
}

// SaveProductionPlan stores or updates a production plan.
func (s *PostgresStore) SaveProductionPlan(ctx context.Context, plan *schedulersvc.ProductionPlan) error {
	batches, err := json.Marshal(plan.Batches)
	if err != nil {
		return fmt.Errorf("failed to marshal batches: %w", err)
	}

	changeovers, err := json.Marshal(plan.ChangeoverWindows)
	if err != nil {
		return fmt.Errorf("failed to marshal changeover windows: %w", err)
	}

	allocations, err := json.Marshal(plan.ResourceAllocations)
	if err != nil {
		return fmt.Errorf("failed to marshal resource allocations: %w", err)
	}

	expectedAdapters := deriveExpectedAdapters(plan)
	if len(expectedAdapters) == 0 {
		expectedAdapters = []string{"plc-p1", "plc-p2"}
	}

	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now()
	}
	plan.CreatedAt = plan.CreatedAt.UTC()

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO scheduler.production_plans (
			plan_id, batches, changeover_windows, start_time, end_time,
			resource_allocations, status, expected_adapters, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		ON CONFLICT (plan_id) DO UPDATE
		SET batches = EXCLUDED.batches,
		    changeover_windows = EXCLUDED.changeover_windows,
		    start_time = EXCLUDED.start_time,
		    end_time = EXCLUDED.end_time,
		    resource_allocations = EXCLUDED.resource_allocations,
		    status = EXCLUDED.status,
		    expected_adapters = EXCLUDED.expected_adapters,
		    updated_at = EXCLUDED.updated_at
	`, plan.PlanID, batches, changeovers, plan.StartTime.UTC(), plan.EndTime.UTC(),
		allocations, plan.Status, pq.Array(expectedAdapters), plan.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to upsert production plan: %w", err)
	}

	return nil
}

// UpdatePlanAck records adapter acknowledgement status.
func (s *PostgresStore) UpdatePlanAck(ctx context.Context, planID, adapterID, result string) error {
	adapterID = strings.ToLower(adapterID)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO scheduler.plan_acks (plan_id, adapter_id, result, ack_timestamp)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (plan_id, adapter_id) DO UPDATE
		SET result = EXCLUDED.result,
		    ack_timestamp = EXCLUDED.ack_timestamp
	`, planID, adapterID, strings.ToUpper(result))
	if err != nil {
		return fmt.Errorf("failed to update plan ack: %w", err)
	}
	return nil
}

// CheckAllAcked verifies if all expected adapters confirmed the plan.
func (s *PostgresStore) CheckAllAcked(ctx context.Context, planID string) (bool, error) {
	var expected []string
	err := s.db.QueryRowContext(ctx, `
		SELECT expected_adapters
		FROM scheduler.production_plans
		WHERE plan_id = $1
	`, planID).Scan(pq.Array(&expected))
	if errors.Is(err, sql.ErrNoRows) {
		return false, schedulersvc.ErrPlanNotFound
	}
	if err != nil {
		return false, fmt.Errorf("failed to query expected adapters: %w", err)
	}

	if len(expected) == 0 {
		return false, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT adapter_id, result
		FROM scheduler.plan_acks
		WHERE plan_id = $1
	`, planID)
	if err != nil {
		return false, fmt.Errorf("failed to query plan acks: %w", err)
	}
	defer rows.Close()

	status := make(map[string]string)
	for rows.Next() {
		var adapterID, result string
		if err := rows.Scan(&adapterID, &result); err != nil {
			return false, fmt.Errorf("failed to scan ack row: %w", err)
		}
		status[strings.ToLower(adapterID)] = strings.ToUpper(result)
	}

	for _, adapter := range expected {
		if status[strings.ToLower(adapter)] != "SUCCESS" {
			return false, nil
		}
	}

	return true, nil
}

// UpdatePlanStatus updates the workflow state for a production plan.
func (s *PostgresStore) UpdatePlanStatus(ctx context.Context, planID, status string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE scheduler.production_plans
		SET status = $2,
		    updated_at = NOW()
		WHERE plan_id = $1
	`, planID, status)
	if err != nil {
		return fmt.Errorf("failed to update plan status: %w", err)
	}

	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return schedulersvc.ErrPlanNotFound
	}

	return err
}

// GetProductionPlan retrieves a stored plan by identifier.
func (s *PostgresStore) GetProductionPlan(ctx context.Context, planID string) (*schedulersvc.ProductionPlan, error) {
	var (
		batchesJSON     []byte
		changeoverJSON  []byte
		allocationsJSON []byte
		plan            schedulersvc.ProductionPlan
	)

	err := s.db.QueryRowContext(ctx, `
		SELECT plan_id, batches, changeover_windows, start_time, end_time,
		       resource_allocations, status, created_at
		FROM scheduler.production_plans
		WHERE plan_id = $1
	`, planID).Scan(
		&plan.PlanID,
		&batchesJSON,
		&changeoverJSON,
		&plan.StartTime,
		&plan.EndTime,
		&allocationsJSON,
		&plan.Status,
		&plan.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, schedulersvc.ErrPlanNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query production plan: %w", err)
	}

	if err := json.Unmarshal(batchesJSON, &plan.Batches); err != nil {
		return nil, fmt.Errorf("failed to unmarshal batches: %w", err)
	}
	if len(changeoverJSON) > 0 {
		if err := json.Unmarshal(changeoverJSON, &plan.ChangeoverWindows); err != nil {
			return nil, fmt.Errorf("failed to unmarshal changeover windows: %w", err)
		}
	}
	if err := json.Unmarshal(allocationsJSON, &plan.ResourceAllocations); err != nil {
		return nil, fmt.Errorf("failed to unmarshal resource allocations: %w", err)
	}

	return &plan, nil
}

func deriveExpectedAdapters(plan *schedulersvc.ProductionPlan) []string {
	adapterSet := map[string]struct{}{}

	for _, allocation := range plan.ResourceAllocations {
		switch strings.ToUpper(allocation.ResourceID) {
		case "PLC_P1":
			adapterSet["plc-p1"] = struct{}{}
		case "PLC_P2":
			adapterSet["plc-p2"] = struct{}{}
		}
	}

	adapters := make([]string, 0, len(adapterSet))
	for adapter := range adapterSet {
		adapters = append(adapters, adapter)
	}
	return adapters
}
