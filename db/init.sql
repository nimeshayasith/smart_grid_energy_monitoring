-- GridPulse PostgreSQL schema
-- Initialised automatically when the postgres container first starts.

-- ── Tariff reference data (updated daily by Airflow) ─────────────────────────
CREATE TABLE IF NOT EXISTS tariff (
    household_id   VARCHAR(20) PRIMARY KEY,
    tariff_rate    DOUBLE PRECISION NOT NULL CHECK (tariff_rate > 0),
    billing_tier   VARCHAR(20)      NOT NULL DEFAULT 'RESIDENTIAL',
    subsidy_flag   BOOLEAN          NOT NULL DEFAULT FALSE,
    updated_at     TIMESTAMP        NOT NULL DEFAULT NOW()
);

-- ── Zone aggregations written by Spark (1-min windows) ───────────────────────
CREATE TABLE IF NOT EXISTS grid_load_by_zone (
    id                    SERIAL PRIMARY KEY,
    grid_zone             VARCHAR(20)      NOT NULL,
    window_start          TIMESTAMP        NOT NULL,
    window_end            TIMESTAMP        NOT NULL,
    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_kwh       DOUBLE PRECISION NOT NULL,
    renewable_pct         DOUBLE PRECISION NOT NULL,
    reading_count         INTEGER          NOT NULL,
    created_at            TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_grid_load_zone       ON grid_load_by_zone (grid_zone);
CREATE INDEX IF NOT EXISTS idx_grid_load_window_start ON grid_load_by_zone (window_start DESC);

-- ── Household billing (5-min windows = 1 simulated day) ─────────────────────
CREATE TABLE IF NOT EXISTS household_billing (
    id                    SERIAL PRIMARY KEY,
    household_id          VARCHAR(20)      NOT NULL,
    billing_date          DATE             NOT NULL,
    window_start          TIMESTAMP        NOT NULL,
    window_end            TIMESTAMP        NOT NULL,
    total_consumption_kwh DOUBLE PRECISION NOT NULL,
    total_solar_kwh       DOUBLE PRECISION NOT NULL,
    tariff_rate           DOUBLE PRECISION NOT NULL,
    billing_tier          VARCHAR(20)      NOT NULL,
    subsidy_flag          BOOLEAN          NOT NULL,
    bill_amount           DOUBLE PRECISION NOT NULL,
    created_at            TIMESTAMP        NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_billing_household ON household_billing (household_id);
CREATE INDEX IF NOT EXISTS idx_billing_date      ON household_billing (billing_date DESC);

-- ── Renewable-shortfall alerts ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS alerts (
    id            SERIAL PRIMARY KEY,
    grid_zone     VARCHAR(20)      NOT NULL,
    window_start  TIMESTAMP        NOT NULL,
    window_end    TIMESTAMP        NOT NULL,
    renewable_pct DOUBLE PRECISION NOT NULL,
    threshold     DOUBLE PRECISION NOT NULL,
    severity      VARCHAR(20)      NOT NULL DEFAULT 'WARNING',
    created_at    TIMESTAMP        NOT NULL DEFAULT NOW(),
    resolved_at   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_zone       ON alerts (grid_zone);
CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_resolved   ON alerts (resolved_at) WHERE resolved_at IS NULL;

-- ── Seed tariff data (30 households, one per meter) ──────────────────────────
-- Ensures billing works from startup before Airflow fires its first run.
-- Airflow will upsert over these with fresh rates every 5 minutes.
INSERT INTO tariff (household_id, tariff_rate, billing_tier, subsidy_flag) VALUES
  ('H001', 0.1423, 'RESIDENTIAL', false), ('H002', 0.1876, 'RESIDENTIAL', false),
  ('H003', 0.1654, 'RESIDENTIAL', true),  ('H004', 0.1234, 'RESIDENTIAL', false),
  ('H005', 0.1987, 'RESIDENTIAL', false), ('H006', 0.1543, 'RESIDENTIAL', true),
  ('H007', 0.1765, 'RESIDENTIAL', false), ('H008', 0.1398, 'RESIDENTIAL', false),
  ('H009', 0.1621, 'RESIDENTIAL', false), ('H010', 0.1845, 'RESIDENTIAL', true),
  ('H011', 0.2134, 'COMMERCIAL',  false), ('H012', 0.2456, 'COMMERCIAL',  false),
  ('H013', 0.1987, 'RESIDENTIAL', false), ('H014', 0.2234, 'COMMERCIAL',  true),
  ('H015', 0.1567, 'RESIDENTIAL', false), ('H016', 0.2678, 'COMMERCIAL',  false),
  ('H017', 0.1432, 'RESIDENTIAL', false), ('H018', 0.1789, 'RESIDENTIAL', true),
  ('H019', 0.2345, 'COMMERCIAL',  false), ('H020', 0.1654, 'RESIDENTIAL', false),
  ('H021', 0.2123, 'COMMERCIAL',  false), ('H022', 0.1876, 'RESIDENTIAL', false),
  ('H023', 0.1543, 'RESIDENTIAL', true),  ('H024', 0.2456, 'COMMERCIAL',  false),
  ('H025', 0.1765, 'RESIDENTIAL', false), ('H026', 0.2234, 'COMMERCIAL',  false),
  ('H027', 0.1398, 'RESIDENTIAL', false), ('H028', 0.1987, 'RESIDENTIAL', true),
  ('H029', 0.2567, 'COMMERCIAL',  false), ('H030', 0.1654, 'RESIDENTIAL', false)
ON CONFLICT (household_id) DO NOTHING;
