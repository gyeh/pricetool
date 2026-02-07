-- Migration: Normalize plan_name into plans table
-- Run via: docker exec -i pricetool-postgres psql -U postgres -d hospital_pricing < migrations/001_normalize_plan_name.sql

BEGIN;

-- 1. Create plans table
CREATE TABLE plans (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- 2. Populate from existing data
INSERT INTO plans (name)
SELECT DISTINCT plan_name FROM payer_charges ORDER BY plan_name;

-- 3. Add plan_id column to payer_charges
ALTER TABLE payer_charges ADD COLUMN plan_id INTEGER;

-- 4. Backfill plan_id (this is the slow step on 47M rows)
UPDATE payer_charges pc
SET plan_id = p.id
FROM plans p
WHERE pc.plan_name = p.name;

-- 5. Add NOT NULL constraint and FK
ALTER TABLE payer_charges ALTER COLUMN plan_id SET NOT NULL;
ALTER TABLE payer_charges ADD CONSTRAINT fk_payer_charges_plan FOREIGN KEY (plan_id) REFERENCES plans(id);
CREATE INDEX idx_payer_charges_plan ON payer_charges(plan_id);

-- 6. Drop old column
ALTER TABLE payer_charges DROP COLUMN plan_name;

-- 7. Migrate modifier_payer_info (likely 0 rows)
ALTER TABLE modifier_payer_info ADD COLUMN plan_id INTEGER;

-- Backfill from plans (insert any missing plan names first)
INSERT INTO plans (name)
SELECT DISTINCT mpi.plan_name
FROM modifier_payer_info mpi
WHERE NOT EXISTS (SELECT 1 FROM plans p WHERE p.name = mpi.plan_name)
ORDER BY mpi.plan_name;

UPDATE modifier_payer_info mpi
SET plan_id = p.id
FROM plans p
WHERE mpi.plan_name = p.name;

-- If there are rows, set NOT NULL; if empty, set NOT NULL directly
ALTER TABLE modifier_payer_info ALTER COLUMN plan_id SET NOT NULL;
ALTER TABLE modifier_payer_info ADD CONSTRAINT fk_modifier_payer_info_plan FOREIGN KEY (plan_id) REFERENCES plans(id);
ALTER TABLE modifier_payer_info DROP COLUMN plan_name;

COMMIT;
