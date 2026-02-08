-- +goose Up

-- Create payers table (normalizes payer_name from payer_charges and modifier_payer_info)
CREATE TABLE IF NOT EXISTS payers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Populate payers from existing payer_charges
INSERT INTO payers (name)
SELECT DISTINCT payer_name FROM payer_charges
ON CONFLICT DO NOTHING;

-- Populate payers from existing modifier_payer_info
INSERT INTO payers (name)
SELECT DISTINCT payer_name FROM modifier_payer_info
WHERE payer_name NOT IN (SELECT name FROM payers)
ON CONFLICT DO NOTHING;

-- Add payer_id to payer_charges
ALTER TABLE payer_charges ADD COLUMN payer_id INTEGER;

UPDATE payer_charges
SET payer_id = payers.id
FROM payers
WHERE payer_charges.payer_name = payers.name;

ALTER TABLE payer_charges ALTER COLUMN payer_id SET NOT NULL;
ALTER TABLE payer_charges
    ADD CONSTRAINT fk_payer_charges_payer FOREIGN KEY (payer_id) REFERENCES payers(id);

DROP INDEX IF EXISTS idx_payer_charges_payer;
ALTER TABLE payer_charges DROP COLUMN payer_name;
CREATE INDEX idx_payer_charges_payer ON payer_charges(payer_id);

-- Add payer_id to modifier_payer_info
ALTER TABLE modifier_payer_info ADD COLUMN payer_id INTEGER;

UPDATE modifier_payer_info
SET payer_id = payers.id
FROM payers
WHERE modifier_payer_info.payer_name = payers.name;

ALTER TABLE modifier_payer_info ALTER COLUMN payer_id SET NOT NULL;
ALTER TABLE modifier_payer_info
    ADD CONSTRAINT fk_modifier_payer_info_payer FOREIGN KEY (payer_id) REFERENCES payers(id);

ALTER TABLE modifier_payer_info DROP COLUMN payer_name;

-- +goose Down

-- Restore payer_name to payer_charges
ALTER TABLE payer_charges ADD COLUMN payer_name VARCHAR(255);

UPDATE payer_charges
SET payer_name = payers.name
FROM payers
WHERE payer_charges.payer_id = payers.id;

ALTER TABLE payer_charges ALTER COLUMN payer_name SET NOT NULL;
DROP INDEX IF EXISTS idx_payer_charges_payer;
ALTER TABLE payer_charges DROP CONSTRAINT fk_payer_charges_payer;
ALTER TABLE payer_charges DROP COLUMN payer_id;
CREATE INDEX idx_payer_charges_payer ON payer_charges(payer_name);

-- Restore payer_name to modifier_payer_info
ALTER TABLE modifier_payer_info ADD COLUMN payer_name VARCHAR(255);

UPDATE modifier_payer_info
SET payer_name = payers.name
FROM payers
WHERE modifier_payer_info.payer_id = payers.id;

ALTER TABLE modifier_payer_info ALTER COLUMN payer_name SET NOT NULL;
ALTER TABLE modifier_payer_info DROP CONSTRAINT fk_modifier_payer_info_payer;
ALTER TABLE modifier_payer_info DROP COLUMN payer_id;

-- Drop payers table
DROP TABLE IF EXISTS payers;
