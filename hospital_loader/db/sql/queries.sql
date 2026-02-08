-- name: InsertHospital :one
INSERT INTO hospitals
  (name, addresses, location_names, npis, license_number, license_state, version, last_updated_on, attester_name)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING id;

-- name: UpsertCode :one
INSERT INTO codes (code, code_type)
VALUES ($1, $2)
ON CONFLICT (code, code_type) DO UPDATE SET code = EXCLUDED.code
RETURNING id;

-- name: InsertItemCode :exec
INSERT INTO item_codes (item_id, code_id)
VALUES ($1, $2)
ON CONFLICT (item_id, code_id) DO NOTHING;

-- name: InsertStandardChargeItem :one
INSERT INTO standard_charge_items (hospital_id, description, drug_unit, drug_unit_type)
VALUES ($1, $2, $3, $4)
RETURNING id;

-- name: InsertStandardCharge :one
INSERT INTO standard_charges
  (item_id, setting, gross_charge, discounted_cash, minimum, maximum, modifier_codes, additional_notes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING id;

-- name: UpsertPlan :one
INSERT INTO plans (name)
VALUES ($1)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;

-- name: UpsertPayer :one
INSERT INTO payers (name)
VALUES ($1)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
RETURNING id;

-- name: InsertPayerCharges :copyfrom
INSERT INTO payer_charges
  (standard_charge_id, payer_id, plan_id, methodology,
   standard_charge_dollar, standard_charge_percentage,
   standard_charge_algorithm, estimated_amount, median_amount,
   percentile_10th, percentile_90th, count, additional_notes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);

-- Read queries (used by tests)

-- name: GetFirstHospital :one
SELECT name, version, addresses[1]::text as first_address FROM hospitals LIMIT 1;

-- name: CountItems :one
SELECT count(*)::int FROM standard_charge_items;

-- name: ListItemDescriptions :many
SELECT description FROM standard_charge_items ORDER BY description;

-- name: CountCodes :one
SELECT count(*)::int FROM codes;

-- name: CodeExists :one
SELECT EXISTS(SELECT 1 FROM codes WHERE code = $1 AND code_type = $2);

-- name: CountItemCodes :one
SELECT count(*)::int FROM item_codes;

-- name: CountCharges :one
SELECT count(*)::int FROM standard_charges;

-- name: ListChargeValues :many
SELECT sci.description, sc.gross_charge
FROM standard_charges sc
JOIN standard_charge_items sci ON sci.id = sc.item_id
ORDER BY sci.description;

-- name: CountPayerCharges :one
SELECT count(*)::int FROM payer_charges;

-- name: ListPayerDetails :many
SELECT py.name as payer_name, p.name as plan_name, pc.standard_charge_dollar, pc.methodology
FROM payer_charges pc
JOIN payers py ON py.id = pc.payer_id
JOIN plans p ON p.id = pc.plan_id
ORDER BY py.name;

-- name: CountPlans :one
SELECT count(*)::int FROM plans;

-- name: CountPayers :one
SELECT count(*)::int FROM payers;

-- name: GetItemDrugInfo :one
SELECT drug_unit, drug_unit_type
FROM standard_charge_items
WHERE description = $1;

-- name: GetItemNotes :one
SELECT sc.additional_notes as generic_notes, pc.additional_notes as payer_notes
FROM standard_charges sc
JOIN standard_charge_items sci ON sci.id = sc.item_id
JOIN payer_charges pc ON pc.standard_charge_id = sc.id
WHERE sci.description = $1;
