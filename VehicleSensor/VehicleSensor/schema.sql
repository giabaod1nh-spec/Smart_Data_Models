-- PostgreSQL schema for VehicleSensor (Smart Traffic Simulation)
-- Aligned with vehiclesensor_model.yaml v1.0.0

CREATE TABLE IF NOT EXISTS vehiclesensor (
    entity_id               TEXT PRIMARY KEY,
    type                    TEXT NOT NULL DEFAULT 'VehicleSensor',
    name                    TEXT,
    description             TEXT,
    date_created            TIMESTAMPTZ,
    date_modified           TIMESTAMPTZ,
    location                GEOGRAPHY(POINT, 4326),
    sensor_type             TEXT,
    sensor_status           TEXT NOT NULL,
    traffic_direction       TEXT NOT NULL,
    ref_intersection        TEXT NOT NULL,
    ref_camera              TEXT,
    ref_traffic_light       TEXT,
    date_observed           TIMESTAMPTZ NOT NULL,
    vehicle_count           INTEGER NOT NULL DEFAULT 0 CHECK (vehicle_count >= 0),
    pcu_equivalent          DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (pcu_equivalent >= 0),
    vehicle_class_composition JSONB,
    left_turn_count         INTEGER DEFAULT 0 CHECK (left_turn_count >= 0),
    straight_count          INTEGER DEFAULT 0 CHECK (straight_count >= 0),
    right_turn_count        INTEGER DEFAULT 0 CHECK (right_turn_count >= 0),
    average_speed           DOUBLE PRECISION CHECK (average_speed >= 0),
    waiting_vehicle_count   INTEGER DEFAULT 0 CHECK (waiting_vehicle_count >= 0),
    queue_length            DOUBLE PRECISION CHECK (queue_length >= 0),
    queue_straight          DOUBLE PRECISION CHECK (queue_straight >= 0),
    queue_left              DOUBLE PRECISION CHECK (queue_left >= 0),
    queue_right             DOUBLE PRECISION CHECK (queue_right >= 0),
    occupancy_rate          DOUBLE PRECISION CHECK (occupancy_rate >= 0 AND occupancy_rate <= 100),
    traffic_status          TEXT,
    arrival_rate_pcu_per_sec DOUBLE PRECISION CHECK (arrival_rate_pcu_per_sec >= 0),
    waiting_reason_counts   JSONB,
    dominant_waiting_reason TEXT,
    theoretical_speed       DOUBLE PRECISION CHECK (theoretical_speed >= 0)
);

CREATE INDEX IF NOT EXISTS idx_vehiclesensor_intersection
    ON vehiclesensor (ref_intersection);
CREATE INDEX IF NOT EXISTS idx_vehiclesensor_direction
    ON vehiclesensor (traffic_direction);
CREATE INDEX IF NOT EXISTS idx_vehiclesensor_observed
    ON vehiclesensor (date_observed);
