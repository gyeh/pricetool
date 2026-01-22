-- Hospitals table
CREATE TABLE IF NOT EXISTS hospitals (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    addresses TEXT[],
    location_names TEXT[],
    npis TEXT[],
    license_number VARCHAR(100),
    license_state VARCHAR(2),
    version VARCHAR(20) NOT NULL,
    last_updated_on DATE NOT NULL,
    attester_name TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Codes table (primary key table as requested)
CREATE TABLE IF NOT EXISTS codes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(100) NOT NULL,
    code_type VARCHAR(20) NOT NULL,
    UNIQUE(code, code_type)
);
CREATE INDEX IF NOT EXISTS idx_codes_code ON codes(code);
CREATE INDEX IF NOT EXISTS idx_codes_type ON codes(code_type);

-- Standard charge items (the services/procedures)
CREATE TABLE IF NOT EXISTS standard_charge_items (
    id SERIAL PRIMARY KEY,
    hospital_id INTEGER NOT NULL REFERENCES hospitals(id) ON DELETE CASCADE,
    description TEXT NOT NULL,
    drug_unit NUMERIC,
    drug_unit_type VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_items_hospital ON standard_charge_items(hospital_id);

-- Junction table linking items to codes
CREATE TABLE IF NOT EXISTS item_codes (
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES standard_charge_items(id) ON DELETE CASCADE,
    code_id INTEGER NOT NULL REFERENCES codes(id) ON DELETE CASCADE,
    UNIQUE(item_id, code_id)
);
CREATE INDEX IF NOT EXISTS idx_item_codes_item ON item_codes(item_id);
CREATE INDEX IF NOT EXISTS idx_item_codes_code ON item_codes(code_id);

-- Standard charges per item/setting
CREATE TABLE IF NOT EXISTS standard_charges (
    id SERIAL PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES standard_charge_items(id) ON DELETE CASCADE,
    setting VARCHAR(20) NOT NULL,
    gross_charge NUMERIC,
    discounted_cash NUMERIC,
    minimum NUMERIC,
    maximum NUMERIC,
    modifier_codes TEXT[],
    additional_notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_charges_item ON standard_charges(item_id);
CREATE INDEX IF NOT EXISTS idx_charges_setting ON standard_charges(setting);

-- Payer-specific charges
CREATE TABLE IF NOT EXISTS payer_charges (
    id SERIAL PRIMARY KEY,
    standard_charge_id INTEGER NOT NULL REFERENCES standard_charges(id) ON DELETE CASCADE,
    payer_name VARCHAR(255) NOT NULL,
    plan_name VARCHAR(255) NOT NULL,
    methodology VARCHAR(50) NOT NULL,
    standard_charge_dollar NUMERIC,
    standard_charge_percentage NUMERIC,
    standard_charge_algorithm TEXT,
    estimated_amount NUMERIC,
    median_amount NUMERIC,
    percentile_10th NUMERIC,
    percentile_90th NUMERIC,
    count VARCHAR(50),
    additional_notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_payer_charges_charge ON payer_charges(standard_charge_id);
CREATE INDEX IF NOT EXISTS idx_payer_charges_payer ON payer_charges(payer_name);

-- Modifiers
CREATE TABLE IF NOT EXISTS modifiers (
    id SERIAL PRIMARY KEY,
    hospital_id INTEGER NOT NULL REFERENCES hospitals(id) ON DELETE CASCADE,
    code VARCHAR(50) NOT NULL,
    description TEXT NOT NULL,
    setting VARCHAR(20)
);
CREATE INDEX IF NOT EXISTS idx_modifiers_hospital ON modifiers(hospital_id);
CREATE INDEX IF NOT EXISTS idx_modifiers_code ON modifiers(code);

-- Modifier payer information
CREATE TABLE IF NOT EXISTS modifier_payer_info (
    id SERIAL PRIMARY KEY,
    modifier_id INTEGER NOT NULL REFERENCES modifiers(id) ON DELETE CASCADE,
    payer_name VARCHAR(255) NOT NULL,
    plan_name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_modifier_payer_modifier ON modifier_payer_info(modifier_id);
