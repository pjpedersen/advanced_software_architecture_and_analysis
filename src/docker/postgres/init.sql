-- I4.0 Production System Database Initialization
-- PostgreSQL schema for Device Registry and Genealogy

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Device Registry Schema
CREATE SCHEMA IF NOT EXISTS device_registry;

-- Genealogy Schema
CREATE SCHEMA IF NOT EXISTS genealogy;

-- Outbox pattern schema
CREATE SCHEMA IF NOT EXISTS outbox;

-------------------------------------
-- DEVICE REGISTRY TABLES
-------------------------------------

CREATE TABLE device_registry.devices (
    device_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    unit_id VARCHAR(255) UNIQUE NOT NULL,
    serial_number VARCHAR(255) UNIQUE NOT NULL,
    pcb_id VARCHAR(255) UNIQUE NOT NULL,

    firmware_version VARCHAR(100) NOT NULL,
    firmware_hash VARCHAR(64) NOT NULL,
    firmware_artifact_id VARCHAR(255),

    identity_type VARCHAR(50) NOT NULL, -- X509_CERT, SYMMETRIC_KEY
    certificate_pem TEXT,
    key_id VARCHAR(255),
    identity_expires_at TIMESTAMP WITH TIME ZONE,

    registered_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, RECALLED, DECOMMISSIONED

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_devices_serial ON device_registry.devices(serial_number);
CREATE INDEX idx_devices_pcb ON device_registry.devices(pcb_id);
CREATE INDEX idx_devices_firmware ON device_registry.devices(firmware_version);
CREATE INDEX idx_devices_status ON device_registry.devices(status);

-- Device validation results
CREATE TABLE device_registry.validation_results (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    test_id VARCHAR(255) NOT NULL,
    test_type VARCHAR(100) NOT NULL, -- ICT, FUNCTIONAL, BOUNDARY_SCAN
    passed BOOLEAN NOT NULL,
    measurements JSONB,

    tested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_validation_device ON device_registry.validation_results(device_id);
CREATE INDEX idx_validation_test_type ON device_registry.validation_results(test_type);
CREATE INDEX idx_validation_passed ON device_registry.validation_results(passed);

-------------------------------------
-- GENEALOGY TABLES
-------------------------------------

CREATE TABLE genealogy.material_genealogy (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    pellet_lot_id VARCHAR(255) NOT NULL,
    pellet_supplier VARCHAR(255),
    pellet_received_date DATE,
    enclosure_color VARCHAR(50) NOT NULL,

    pcb_batch_id VARCHAR(255) NOT NULL,
    pcb_supplier VARCHAR(255),
    pcb_manufactured_date DATE,

    components JSONB, -- Array of {part_number, lot_id, supplier, date_code}

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_material_device ON genealogy.material_genealogy(device_id);
CREATE INDEX idx_material_pellet_lot ON genealogy.material_genealogy(pellet_lot_id);
CREATE INDEX idx_material_pcb_batch ON genealogy.material_genealogy(pcb_batch_id);
CREATE INDEX idx_material_color ON genealogy.material_genealogy(enclosure_color);

CREATE TABLE genealogy.process_genealogy (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    process_steps JSONB NOT NULL, -- Array of process steps
    station_ids JSONB,
    operator_ids JSONB,

    production_started_at TIMESTAMP WITH TIME ZONE,
    production_completed_at TIMESTAMP WITH TIME ZONE,
    total_duration_seconds INTEGER,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_process_device ON genealogy.process_genealogy(device_id);

CREATE TABLE genealogy.software_genealogy (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    firmware_version VARCHAR(100) NOT NULL,
    firmware_hash VARCHAR(64) NOT NULL,
    firmware_artifact_id VARCHAR(255),
    build_id VARCHAR(255),
    build_timestamp TIMESTAMP WITH TIME ZONE,
    git_commit_sha VARCHAR(40),
    dependencies JSONB,

    flashed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_software_device ON genealogy.software_genealogy(device_id);
CREATE INDEX idx_software_firmware ON genealogy.software_genealogy(firmware_version);
CREATE INDEX idx_software_hash ON genealogy.software_genealogy(firmware_hash);

CREATE TABLE genealogy.shipping_info (
    id BIGSERIAL PRIMARY KEY,
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    shipment_id VARCHAR(255) NOT NULL,
    shipping_lot VARCHAR(255),
    customer_order_id VARCHAR(255),
    customer_id VARCHAR(255) NOT NULL,

    shipped_at TIMESTAMP WITH TIME ZONE NOT NULL,
    destination VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_shipping_device ON genealogy.shipping_info(device_id);
CREATE INDEX idx_shipping_customer ON genealogy.shipping_info(customer_id);
CREATE INDEX idx_shipping_lot ON genealogy.shipping_info(shipping_lot);

-- Recall tracking
CREATE TABLE genealogy.recalls (
    id BIGSERIAL PRIMARY KEY,
    recall_id UUID UNIQUE NOT NULL DEFAULT uuid_generate_v4(),

    title VARCHAR(255) NOT NULL,
    description TEXT,
    severity VARCHAR(50) NOT NULL, -- CRITICAL, MAJOR, MINOR

    criteria JSONB NOT NULL, -- Search criteria for affected devices
    affected_device_count INTEGER,

    initiated_by VARCHAR(255) NOT NULL,
    initiated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE', -- ACTIVE, COMPLETED, CANCELLED

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE genealogy.recall_devices (
    id BIGSERIAL PRIMARY KEY,
    recall_id UUID NOT NULL REFERENCES genealogy.recalls(recall_id),
    device_id UUID NOT NULL REFERENCES device_registry.devices(device_id),

    match_reasons TEXT[],
    notification_sent BOOLEAN DEFAULT FALSE,
    notification_sent_at TIMESTAMP WITH TIME ZONE,

    remediation_status VARCHAR(50) DEFAULT 'PENDING', -- PENDING, NOTIFIED, REMEDIATED
    remediated_at TIMESTAMP WITH TIME ZONE,

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    UNIQUE(recall_id, device_id)
);

CREATE INDEX idx_recall_devices_recall ON genealogy.recall_devices(recall_id);
CREATE INDEX idx_recall_devices_device ON genealogy.recall_devices(device_id);
CREATE INDEX idx_recall_devices_status ON genealogy.recall_devices(remediation_status);

-------------------------------------
-- TRANSACTIONAL OUTBOX
-------------------------------------

CREATE TABLE outbox.events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL DEFAULT uuid_generate_v4(),
    event_type VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,

    payload JSONB NOT NULL,
    correlation_id VARCHAR(255),

    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER DEFAULT 0,
    last_error TEXT
);

CREATE INDEX idx_outbox_published ON outbox.events(published_at) WHERE published_at IS NULL;
CREATE INDEX idx_outbox_created ON outbox.events(created_at);
CREATE INDEX idx_outbox_type ON outbox.events(event_type);

-------------------------------------
-- DEDUPLICATION STORE
-------------------------------------

CREATE TABLE outbox.dedup_store (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    event_type VARCHAR(255) NOT NULL,
    result JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_dedup_expires ON outbox.dedup_store(expires_at);

-------------------------------------
-- FUNCTIONS AND TRIGGERS
-------------------------------------

-- Updated timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_devices_updated_at
    BEFORE UPDATE ON device_registry.devices
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_recalls_updated_at
    BEFORE UPDATE ON genealogy.recalls
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Cleanup old dedup entries
CREATE OR REPLACE FUNCTION cleanup_expired_dedup()
RETURNS void AS $$
BEGIN
    DELETE FROM outbox.dedup_store WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

-------------------------------------
-- INITIAL DATA
-------------------------------------

-- Grant permissions (for development)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA device_registry TO i40admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA device_registry TO i40admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA genealogy TO i40admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA genealogy TO i40admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA outbox TO i40admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA outbox TO i40admin;

-- Create read-only user for reporting
CREATE USER i40_readonly WITH PASSWORD 'readonly';
GRANT CONNECT ON DATABASE i40_production TO i40_readonly;
GRANT USAGE ON SCHEMA device_registry, genealogy TO i40_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA device_registry, genealogy TO i40_readonly;
