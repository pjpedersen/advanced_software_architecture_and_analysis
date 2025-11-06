package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Store defines the interface for template storage operations
type Store interface {
	SaveTemplate(ctx context.Context, template *CustomizationTemplate) error
	GetTemplate(ctx context.Context, templateID string) (*CustomizationTemplate, error)
	GetTemplateByOrder(ctx context.Context, customerOrder string) (*CustomizationTemplate, error)
	ListTemplates(ctx context.Context, limit, offset int) ([]*CustomizationTemplate, error)
	UpdateTemplateStatus(ctx context.Context, templateID, status string) error
	SaveEngravingJob(ctx context.Context, job *EngravingJob) error
	GetEngravingJob(ctx context.Context, jobID string) (*EngravingJob, error)
	Close() error
}

// CustomizationTemplate represents a customer engraving template
type CustomizationTemplate struct {
	TemplateID    string                 `json:"template_id"`
	CustomerOrder string                 `json:"customer_order"`
	CustomerName  string                 `json:"customer_name"`
	EngravingText string                 `json:"engraving_text"`
	LogoData      []byte                 `json:"logo_data,omitempty"`
	FontStyle     string                 `json:"font_style"`
	LayoutParams  map[string]interface{} `json:"layout_params"`
	Status        string                 `json:"status"` // DRAFT, APPROVED, ACTIVE, ARCHIVED
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
}

// EngravingJob represents a specific engraving job for a device
type EngravingJob struct {
	JobID         string                 `json:"job_id"`
	DeviceID      string                 `json:"device_id"`
	UnitID        string                 `json:"unit_id"`
	TemplateID    string                 `json:"template_id"`
	CustomerOrder string                 `json:"customer_order"`
	EngravingData map[string]interface{} `json:"engraving_data"`
	Status        string                 `json:"status"` // PENDING, IN_PROGRESS, COMPLETED, FAILED
	StartedAt     *time.Time             `json:"started_at,omitempty"`
	CompletedAt   *time.Time             `json:"completed_at,omitempty"`
	ErrorMessage  string                 `json:"error_message,omitempty"`
	CreatedAt     time.Time              `json:"created_at"`
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

	// Create tables if they don't exist
	if err := createTables(db); err != nil {
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	return &postgresStore{db: db}, nil
}

func createTables(db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS customization_templates (
		template_id VARCHAR(255) PRIMARY KEY,
		customer_order VARCHAR(255) NOT NULL UNIQUE,
		customer_name VARCHAR(255) NOT NULL,
		engraving_text TEXT NOT NULL,
		logo_data BYTEA,
		font_style VARCHAR(100),
		layout_params JSONB,
		status VARCHAR(50) NOT NULL DEFAULT 'DRAFT',
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_templates_customer_order ON customization_templates(customer_order);
	CREATE INDEX IF NOT EXISTS idx_templates_status ON customization_templates(status);

	CREATE TABLE IF NOT EXISTS engraving_jobs (
		job_id VARCHAR(255) PRIMARY KEY,
		device_id VARCHAR(255) NOT NULL,
		unit_id VARCHAR(255) NOT NULL,
		template_id VARCHAR(255) NOT NULL,
		customer_order VARCHAR(255) NOT NULL,
		engraving_data JSONB NOT NULL,
		status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
		started_at TIMESTAMPTZ,
		completed_at TIMESTAMPTZ,
		error_message TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_jobs_device_id ON engraving_jobs(device_id);
	CREATE INDEX IF NOT EXISTS idx_jobs_template_id ON engraving_jobs(template_id);
	CREATE INDEX IF NOT EXISTS idx_jobs_status ON engraving_jobs(status);
	`

	_, err := db.Exec(schema)
	return err
}

// SaveTemplate saves a customization template
func (s *postgresStore) SaveTemplate(ctx context.Context, template *CustomizationTemplate) error {
	query := `
		INSERT INTO customization_templates (
			template_id, customer_order, customer_name, engraving_text,
			logo_data, font_style, layout_params, status, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (template_id) DO UPDATE SET
			engraving_text = EXCLUDED.engraving_text,
			logo_data = EXCLUDED.logo_data,
			font_style = EXCLUDED.font_style,
			layout_params = EXCLUDED.layout_params,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at
	`

	layoutParamsJSON, _ := json.Marshal(template.LayoutParams)

	_, err := s.db.ExecContext(ctx, query,
		template.TemplateID,
		template.CustomerOrder,
		template.CustomerName,
		template.EngravingText,
		template.LogoData,
		template.FontStyle,
		layoutParamsJSON,
		template.Status,
		template.CreatedAt,
		template.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to save template: %w", err)
	}

	return nil
}

// GetTemplate retrieves a template by ID
func (s *postgresStore) GetTemplate(ctx context.Context, templateID string) (*CustomizationTemplate, error) {
	query := `
		SELECT template_id, customer_order, customer_name, engraving_text,
		       logo_data, font_style, layout_params, status, created_at, updated_at
		FROM customization_templates
		WHERE template_id = $1
	`

	var template CustomizationTemplate
	var layoutParamsJSON []byte

	err := s.db.QueryRowContext(ctx, query, templateID).Scan(
		&template.TemplateID,
		&template.CustomerOrder,
		&template.CustomerName,
		&template.EngravingText,
		&template.LogoData,
		&template.FontStyle,
		&layoutParamsJSON,
		&template.Status,
		&template.CreatedAt,
		&template.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("template not found: %s", templateID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %w", err)
	}

	if len(layoutParamsJSON) > 0 {
		json.Unmarshal(layoutParamsJSON, &template.LayoutParams)
	}

	return &template, nil
}

// GetTemplateByOrder retrieves a template by customer order
func (s *postgresStore) GetTemplateByOrder(ctx context.Context, customerOrder string) (*CustomizationTemplate, error) {
	query := `
		SELECT template_id, customer_order, customer_name, engraving_text,
		       logo_data, font_style, layout_params, status, created_at, updated_at
		FROM customization_templates
		WHERE customer_order = $1
	`

	var template CustomizationTemplate
	var layoutParamsJSON []byte

	err := s.db.QueryRowContext(ctx, query, customerOrder).Scan(
		&template.TemplateID,
		&template.CustomerOrder,
		&template.CustomerName,
		&template.EngravingText,
		&template.LogoData,
		&template.FontStyle,
		&layoutParamsJSON,
		&template.Status,
		&template.CreatedAt,
		&template.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("template not found for order: %s", customerOrder)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get template by order: %w", err)
	}

	if len(layoutParamsJSON) > 0 {
		json.Unmarshal(layoutParamsJSON, &template.LayoutParams)
	}

	return &template, nil
}

// ListTemplates lists templates with pagination
func (s *postgresStore) ListTemplates(ctx context.Context, limit, offset int) ([]*CustomizationTemplate, error) {
	query := `
		SELECT template_id, customer_order, customer_name, engraving_text,
		       logo_data, font_style, layout_params, status, created_at, updated_at
		FROM customization_templates
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := s.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}
	defer rows.Close()

	var templates []*CustomizationTemplate
	for rows.Next() {
		var template CustomizationTemplate
		var layoutParamsJSON []byte

		err := rows.Scan(
			&template.TemplateID,
			&template.CustomerOrder,
			&template.CustomerName,
			&template.EngravingText,
			&template.LogoData,
			&template.FontStyle,
			&layoutParamsJSON,
			&template.Status,
			&template.CreatedAt,
			&template.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan template: %w", err)
		}

		if len(layoutParamsJSON) > 0 {
			json.Unmarshal(layoutParamsJSON, &template.LayoutParams)
		}

		templates = append(templates, &template)
	}

	return templates, rows.Err()
}

// UpdateTemplateStatus updates the status of a template
func (s *postgresStore) UpdateTemplateStatus(ctx context.Context, templateID, status string) error {
	query := `
		UPDATE customization_templates
		SET status = $1, updated_at = NOW()
		WHERE template_id = $2
	`

	result, err := s.db.ExecContext(ctx, query, status, templateID)
	if err != nil {
		return fmt.Errorf("failed to update template status: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("template not found: %s", templateID)
	}

	return nil
}

// SaveEngravingJob saves an engraving job
func (s *postgresStore) SaveEngravingJob(ctx context.Context, job *EngravingJob) error {
	query := `
		INSERT INTO engraving_jobs (
			job_id, device_id, unit_id, template_id, customer_order,
			engraving_data, status, started_at, completed_at, error_message, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (job_id) DO UPDATE SET
			status = EXCLUDED.status,
			started_at = EXCLUDED.started_at,
			completed_at = EXCLUDED.completed_at,
			error_message = EXCLUDED.error_message
	`

	engravingDataJSON, _ := json.Marshal(job.EngravingData)

	_, err := s.db.ExecContext(ctx, query,
		job.JobID,
		job.DeviceID,
		job.UnitID,
		job.TemplateID,
		job.CustomerOrder,
		engravingDataJSON,
		job.Status,
		job.StartedAt,
		job.CompletedAt,
		job.ErrorMessage,
		job.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to save engraving job: %w", err)
	}

	return nil
}

// GetEngravingJob retrieves an engraving job by ID
func (s *postgresStore) GetEngravingJob(ctx context.Context, jobID string) (*EngravingJob, error) {
	query := `
		SELECT job_id, device_id, unit_id, template_id, customer_order,
		       engraving_data, status, started_at, completed_at, error_message, created_at
		FROM engraving_jobs
		WHERE job_id = $1
	`

	var job EngravingJob
	var engravingDataJSON []byte

	err := s.db.QueryRowContext(ctx, query, jobID).Scan(
		&job.JobID,
		&job.DeviceID,
		&job.UnitID,
		&job.TemplateID,
		&job.CustomerOrder,
		&engravingDataJSON,
		&job.Status,
		&job.StartedAt,
		&job.CompletedAt,
		&job.ErrorMessage,
		&job.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("engraving job not found: %s", jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get engraving job: %w", err)
	}

	if len(engravingDataJSON) > 0 {
		json.Unmarshal(engravingDataJSON, &job.EngravingData)
	}

	return &job, nil
}

// Close closes the database connection
func (s *postgresStore) Close() error {
	return s.db.Close()
}
