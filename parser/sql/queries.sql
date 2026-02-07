-- name: InsertHospital :one
INSERT INTO hospitals (
    name, addresses, location_names, npis,
    license_number, license_state, version,
    last_updated_on, attester_name
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;

-- name: UpsertCode :one
INSERT INTO codes (code, code_type)
VALUES ($1, $2)
ON CONFLICT (code, code_type) DO UPDATE SET code = EXCLUDED.code
RETURNING id;

-- name: UpsertPlan :one
INSERT INTO plans (name)
VALUES ($1)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;

-- name: InsertStandardChargeItem :one
INSERT INTO standard_charge_items (hospital_id, description, drug_unit, drug_unit_type)
VALUES ($1, $2, $3, $4)
RETURNING id;

-- name: InsertItemCode :exec
INSERT INTO item_codes (item_id, code_id)
VALUES ($1, $2)
ON CONFLICT (item_id, code_id) DO NOTHING;

-- name: InsertStandardCharge :one
INSERT INTO standard_charges (
    item_id, setting, gross_charge, discounted_cash,
    minimum, maximum, modifier_codes, additional_notes
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id;

-- name: InsertPayerCharge :exec
INSERT INTO payer_charges (
    standard_charge_id, payer_name, plan_id, methodology,
    standard_charge_dollar, standard_charge_percentage,
    standard_charge_algorithm, estimated_amount, median_amount,
    percentile_10th, percentile_90th, count, additional_notes
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);

-- name: InsertModifier :one
INSERT INTO modifiers (hospital_id, code, description, setting)
VALUES ($1, $2, $3, $4)
RETURNING id;

-- name: InsertModifierPayerInfo :exec
INSERT INTO modifier_payer_info (modifier_id, payer_name, plan_id, description)
VALUES ($1, $2, $3, $4);

-- name: GetCodeByCodeAndType :one
SELECT id, code, code_type FROM codes
WHERE code = $1 AND code_type = $2;

-- name: GetHospitalByName :one
SELECT * FROM hospitals WHERE name = $1;

-- name: ListCodesByType :many
SELECT id, code, code_type FROM codes
WHERE code_type = $1
ORDER BY code;

-- name: ListStandardChargeItemsByCode :many
SELECT sci.* FROM standard_charge_items sci
JOIN item_codes ic ON ic.item_id = sci.id
JOIN codes c ON c.id = ic.code_id
WHERE c.code = $1 AND c.code_type = $2;
