package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"pricetool/db"
)

// testDB holds the embedded postgres instance and connection pool
type testDB struct {
	postgres *embeddedpostgres.EmbeddedPostgres
	pool     *pgxpool.Pool
}

// setupTestDB creates a fresh embedded PostgreSQL database for testing
func setupTestDB(t *testing.T) *testDB {
	t.Helper()

	postgres := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("test").
		Password("test").
		Database("test").
		Port(15432).
		StartTimeout(60 * time.Second))

	if err := postgres.Start(); err != nil {
		t.Fatalf("Failed to start embedded postgres: %v", err)
	}

	ctx := context.Background()
	connStr := "postgres://test:test@localhost:15432/test?sslmode=disable"

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		postgres.Stop()
		t.Fatalf("Failed to connect to embedded postgres: %v", err)
	}

	// Initialize schema
	if err := initializeSchema(ctx, pool); err != nil {
		pool.Close()
		postgres.Stop()
		t.Fatalf("Failed to initialize schema: %v", err)
	}

	return &testDB{
		postgres: postgres,
		pool:     pool,
	}
}

// teardown stops the embedded database
func (tdb *testDB) teardown() {
	if tdb.pool != nil {
		tdb.pool.Close()
	}
	if tdb.postgres != nil {
		tdb.postgres.Stop()
	}
}

// cleanup removes all data from tables (for use between subtests)
func (tdb *testDB) cleanup(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	tables := []string{
		"modifier_payer_info",
		"modifiers",
		"payer_charges",
		"standard_charges",
		"item_codes",
		"standard_charge_items",
		"codes",
		"hospitals",
	}

	for _, table := range tables {
		_, err := tdb.pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s CASCADE", table))
		if err != nil {
			t.Logf("Warning: failed to truncate table %s: %v", table, err)
		}
	}
}

func TestInsertHospital(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	date, _ := time.Parse("2006-01-02", "2025-01-15")
	params := db.InsertHospitalParams{
		Name:          "Test Hospital",
		Addresses:     []string{"123 Main St", "456 Oak Ave"},
		LocationNames: []string{"Main Campus", "Surgery Center"},
		Npis:          []string{"1234567890", "0987654321"},
		LicenseNumber: pgtype.Text{String: "LIC123", Valid: true},
		LicenseState:  pgtype.Text{String: "CA", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "John Doe", Valid: true},
	}

	id, err := queries.InsertHospital(ctx, params)
	if err != nil {
		t.Fatalf("Failed to insert hospital: %v", err)
	}

	if id <= 0 {
		t.Errorf("Expected positive hospital ID, got %d", id)
	}

	// Verify by fetching
	hospital, err := queries.GetHospitalByName(ctx, "Test Hospital")
	if err != nil {
		t.Fatalf("Failed to get hospital: %v", err)
	}

	if hospital.Name != "Test Hospital" {
		t.Errorf("Expected name 'Test Hospital', got '%s'", hospital.Name)
	}
	if hospital.LicenseState.String != "CA" {
		t.Errorf("Expected license state 'CA', got '%s'", hospital.LicenseState.String)
	}
	if len(hospital.Addresses) != 2 {
		t.Errorf("Expected 2 addresses, got %d", len(hospital.Addresses))
	}
}

func TestInsertHospitalWithNullLicense(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	date, _ := time.Parse("2006-01-02", "2025-01-15")
	params := db.InsertHospitalParams{
		Name:          "Hospital Without License Number",
		Addresses:     []string{"123 Main St"},
		LocationNames: []string{"Main Campus"},
		Npis:          []string{"1234567890"},
		LicenseNumber: pgtype.Text{Valid: false}, // NULL
		LicenseState:  pgtype.Text{String: "NY", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "Jane Doe", Valid: true},
	}

	id, err := queries.InsertHospital(ctx, params)
	if err != nil {
		t.Fatalf("Failed to insert hospital with null license: %v", err)
	}

	if id <= 0 {
		t.Errorf("Expected positive hospital ID, got %d", id)
	}
}

func TestUpsertCode(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	// Insert first code
	id1, err := queries.UpsertCode(ctx, db.UpsertCodeParams{
		Code:     "470",
		CodeType: "MS-DRG",
	})
	if err != nil {
		t.Fatalf("Failed to insert code: %v", err)
	}

	// Upsert same code - should return same ID
	id2, err := queries.UpsertCode(ctx, db.UpsertCodeParams{
		Code:     "470",
		CodeType: "MS-DRG",
	})
	if err != nil {
		t.Fatalf("Failed to upsert code: %v", err)
	}

	if id1 != id2 {
		t.Errorf("Expected same ID for upsert, got %d and %d", id1, id2)
	}

	// Insert different code type with same code
	id3, err := queries.UpsertCode(ctx, db.UpsertCodeParams{
		Code:     "470",
		CodeType: "LOCAL",
	})
	if err != nil {
		t.Fatalf("Failed to insert different code type: %v", err)
	}

	if id3 == id1 {
		t.Errorf("Expected different ID for different code type, got same: %d", id3)
	}
}

func TestInsertStandardChargeItem(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	// First insert a hospital
	hospitalID := insertTestHospital(t, queries)

	// Insert standard charge item
	itemID, err := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Major hip replacement",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})
	if err != nil {
		t.Fatalf("Failed to insert standard charge item: %v", err)
	}

	if itemID <= 0 {
		t.Errorf("Expected positive item ID, got %d", itemID)
	}
}

func TestInsertStandardChargeItemWithDrug(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	// Insert drug item
	drugUnit := toNumeric(ptrFloat(100))
	itemID, err := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Aspirin 81mg tablet",
		DrugUnit:     drugUnit,
		DrugUnitType: pgtype.Text{String: "UN", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert drug item: %v", err)
	}

	if itemID <= 0 {
		t.Errorf("Expected positive item ID, got %d", itemID)
	}
}

func TestInsertItemCode(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	itemID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Test Item",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})

	codeID, _ := queries.UpsertCode(ctx, db.UpsertCodeParams{
		Code:     "470",
		CodeType: "MS-DRG",
	})

	// Test linking item to code
	err := queries.InsertItemCode(ctx, db.InsertItemCodeParams{
		ItemID: itemID,
		CodeID: codeID,
	})
	if err != nil {
		t.Fatalf("Failed to insert item code link: %v", err)
	}

	// Test duplicate insert (should not error due to ON CONFLICT DO NOTHING)
	err = queries.InsertItemCode(ctx, db.InsertItemCodeParams{
		ItemID: itemID,
		CodeID: codeID,
	})
	if err != nil {
		t.Fatalf("Duplicate item code insert should not error: %v", err)
	}
}

func TestInsertStandardCharge(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	itemID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Test Item",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})

	// Insert standard charge
	chargeID, err := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
		ItemID:          itemID,
		Setting:         "inpatient",
		GrossCharge:     toNumeric(ptrFloat(50000)),
		DiscountedCash:  toNumeric(ptrFloat(40000)),
		Minimum:         toNumeric(ptrFloat(25000)),
		Maximum:         toNumeric(ptrFloat(30000)),
		ModifierCodes:   []string{"50", "62"},
		AdditionalNotes: pgtype.Text{String: "Test notes", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert standard charge: %v", err)
	}

	if chargeID <= 0 {
		t.Errorf("Expected positive charge ID, got %d", chargeID)
	}
}

func TestInsertStandardChargeWithNulls(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	itemID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Test Item",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})

	// Insert with minimal fields (nulls for optional)
	chargeID, err := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
		ItemID:          itemID,
		Setting:         "outpatient",
		GrossCharge:     pgtype.Numeric{Valid: false},
		DiscountedCash:  pgtype.Numeric{Valid: false},
		Minimum:         pgtype.Numeric{Valid: false},
		Maximum:         pgtype.Numeric{Valid: false},
		ModifierCodes:   nil,
		AdditionalNotes: pgtype.Text{Valid: false},
	})
	if err != nil {
		t.Fatalf("Failed to insert standard charge with nulls: %v", err)
	}

	if chargeID <= 0 {
		t.Errorf("Expected positive charge ID, got %d", chargeID)
	}
}

func TestInsertPayerCharge(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	itemID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Test Item",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})

	chargeID, _ := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
		ItemID:  itemID,
		Setting: "inpatient",
	})

	// Insert payer charge with dollar amount
	err := queries.InsertPayerCharge(ctx, db.InsertPayerChargeParams{
		StandardChargeID:         chargeID,
		PayerName:                "Test Insurance",
		PlanName:                 "PPO",
		Methodology:              "case rate",
		StandardChargeDollar:     toNumeric(ptrFloat(25000)),
		StandardChargePercentage: pgtype.Numeric{Valid: false},
		StandardChargeAlgorithm:  pgtype.Text{Valid: false},
		MedianAmount:             pgtype.Numeric{Valid: false},
		Percentile10th:           pgtype.Numeric{Valid: false},
		Percentile90th:           pgtype.Numeric{Valid: false},
		Count:                    pgtype.Text{Valid: false},
		AdditionalNotes:          pgtype.Text{Valid: false},
	})
	if err != nil {
		t.Fatalf("Failed to insert payer charge: %v", err)
	}
}

func TestInsertPayerChargeWithPercentage(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	itemID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  "Test Item",
		DrugUnit:     pgtype.Numeric{Valid: false},
		DrugUnitType: pgtype.Text{Valid: false},
	})

	chargeID, _ := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
		ItemID:  itemID,
		Setting: "inpatient",
	})

	// Insert payer charge with percentage
	err := queries.InsertPayerCharge(ctx, db.InsertPayerChargeParams{
		StandardChargeID:         chargeID,
		PayerName:                "Test Insurance",
		PlanName:                 "HMO",
		Methodology:              "percent of total billed charges",
		StandardChargeDollar:     pgtype.Numeric{Valid: false},
		StandardChargePercentage: toNumeric(ptrFloat(50)),
		StandardChargeAlgorithm:  pgtype.Text{Valid: false},
		MedianAmount:             toNumeric(ptrFloat(21345.12)),
		Percentile10th:           toNumeric(ptrFloat(18765.90)),
		Percentile90th:           toNumeric(ptrFloat(39627.88)),
		Count:                    pgtype.Text{String: "23", Valid: true},
		AdditionalNotes:          pgtype.Text{Valid: false},
	})
	if err != nil {
		t.Fatalf("Failed to insert payer charge with percentage: %v", err)
	}
}

func TestInsertModifier(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	// Insert modifier
	modifierID, err := queries.InsertModifier(ctx, db.InsertModifierParams{
		HospitalID:  hospitalID,
		Code:        "50",
		Description: "Bilateral procedure",
		Setting:     pgtype.Text{String: "both", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert modifier: %v", err)
	}

	if modifierID <= 0 {
		t.Errorf("Expected positive modifier ID, got %d", modifierID)
	}
}

func TestInsertModifierPayerInfo(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	modifierID, _ := queries.InsertModifier(ctx, db.InsertModifierParams{
		HospitalID:  hospitalID,
		Code:        "50",
		Description: "Bilateral procedure",
		Setting:     pgtype.Text{String: "both", Valid: true},
	})

	// Insert modifier payer info
	err := queries.InsertModifierPayerInfo(ctx, db.InsertModifierPayerInfoParams{
		ModifierID:  modifierID,
		PayerName:   "Test Insurance",
		PlanName:    "PPO",
		Description: "150% payment adjustment",
	})
	if err != nil {
		t.Fatalf("Failed to insert modifier payer info: %v", err)
	}
}

func TestListCodesByType(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	// Insert multiple codes
	queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "470", CodeType: "MS-DRG"})
	queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "471", CodeType: "MS-DRG"})
	queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "472", CodeType: "MS-DRG"})
	queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "99213", CodeType: "CPT"})

	// List MS-DRG codes
	codes, err := queries.ListCodesByType(ctx, "MS-DRG")
	if err != nil {
		t.Fatalf("Failed to list codes: %v", err)
	}

	if len(codes) != 3 {
		t.Errorf("Expected 3 MS-DRG codes, got %d", len(codes))
	}

	// Verify ordering
	if codes[0].Code != "470" {
		t.Errorf("Expected first code '470', got '%s'", codes[0].Code)
	}
}

func TestListStandardChargeItemsByCode(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	// Insert items and link to codes
	item1ID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:  hospitalID,
		Description: "Hip replacement",
	})
	item2ID, _ := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:  hospitalID,
		Description: "Knee replacement",
	})

	codeID, _ := queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "470", CodeType: "MS-DRG"})
	queries.InsertItemCode(ctx, db.InsertItemCodeParams{ItemID: item1ID, CodeID: codeID})
	queries.InsertItemCode(ctx, db.InsertItemCodeParams{ItemID: item2ID, CodeID: codeID})

	// Query by code
	items, err := queries.ListStandardChargeItemsByCode(ctx, db.ListStandardChargeItemsByCodeParams{
		Code:     "470",
		CodeType: "MS-DRG",
	})
	if err != nil {
		t.Fatalf("Failed to list items by code: %v", err)
	}

	if len(items) != 2 {
		t.Errorf("Expected 2 items for code 470, got %d", len(items))
	}
}

func TestFullImportFlow(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	// Simulate full import flow
	date, _ := time.Parse("2006-01-02", "2025-01-15")

	// 1. Insert hospital
	hospitalID, err := queries.InsertHospital(ctx, db.InsertHospitalParams{
		Name:          "West Mercy Hospital",
		Addresses:     []string{"12 Main Street, Fullerton, CA 92832"},
		LocationNames: []string{"West Mercy Hospital", "West Mercy Surgical Center"},
		Npis:          []string{"0000000001", "0000000002"},
		LicenseNumber: pgtype.Text{String: "50056", Valid: true},
		LicenseState:  pgtype.Text{String: "CA", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "Leigh Attester", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert hospital: %v", err)
	}

	// 2. Insert standard charge item
	itemID, err := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:  hospitalID,
		Description: "Major hip and knee joint replacement",
	})
	if err != nil {
		t.Fatalf("Failed to insert item: %v", err)
	}

	// 3. Insert and link codes
	code1ID, _ := queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "470", CodeType: "MS-DRG"})
	code2ID, _ := queries.UpsertCode(ctx, db.UpsertCodeParams{Code: "175869", CodeType: "LOCAL"})
	queries.InsertItemCode(ctx, db.InsertItemCodeParams{ItemID: itemID, CodeID: code1ID})
	queries.InsertItemCode(ctx, db.InsertItemCodeParams{ItemID: itemID, CodeID: code2ID})

	// 4. Insert standard charge
	chargeID, err := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
		ItemID:  itemID,
		Setting: "inpatient",
		Minimum: toNumeric(ptrFloat(25678)),
		Maximum: toNumeric(ptrFloat(25678)),
	})
	if err != nil {
		t.Fatalf("Failed to insert charge: %v", err)
	}

	// 5. Insert payer charges
	err = queries.InsertPayerCharge(ctx, db.InsertPayerChargeParams{
		StandardChargeID:         chargeID,
		PayerName:                "Platform Health Insurance",
		PlanName:                 "PPO",
		Methodology:              "percent of total billed charges",
		StandardChargePercentage: toNumeric(ptrFloat(50)),
		MedianAmount:             toNumeric(ptrFloat(21345.12)),
		Percentile10th:           toNumeric(ptrFloat(18765.90)),
		Percentile90th:           toNumeric(ptrFloat(39627.88)),
		Count:                    pgtype.Text{String: "23", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert payer charge: %v", err)
	}

	// 6. Insert modifier
	modifierID, err := queries.InsertModifier(ctx, db.InsertModifierParams{
		HospitalID:  hospitalID,
		Code:        "50",
		Description: "Bilateral procedure",
		Setting:     pgtype.Text{String: "both", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert modifier: %v", err)
	}

	// 7. Insert modifier payer info
	err = queries.InsertModifierPayerInfo(ctx, db.InsertModifierPayerInfoParams{
		ModifierID:  modifierID,
		PayerName:   "Platform Health Insurance",
		PlanName:    "PPO",
		Description: "150% payment adjustment",
	})
	if err != nil {
		t.Fatalf("Failed to insert modifier payer info: %v", err)
	}

	// Verify the full chain
	hospital, _ := queries.GetHospitalByName(ctx, "West Mercy Hospital")
	if hospital.ID != hospitalID {
		t.Errorf("Hospital ID mismatch")
	}

	items, _ := queries.ListStandardChargeItemsByCode(ctx, db.ListStandardChargeItemsByCodeParams{
		Code:     "470",
		CodeType: "MS-DRG",
	})
	if len(items) != 1 {
		t.Errorf("Expected 1 item linked to code 470, got %d", len(items))
	}
}

func TestTransactionRollback(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()

	// Start transaction
	tx, err := tdb.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	queries := db.New(tx)

	// Insert hospital in transaction
	date, _ := time.Parse("2006-01-02", "2025-01-15")
	_, err = queries.InsertHospital(ctx, db.InsertHospitalParams{
		Name:          "Rollback Test Hospital",
		Addresses:     []string{"123 Main St"},
		LocationNames: []string{"Main"},
		Npis:          []string{"1234567890"},
		LicenseNumber: pgtype.Text{Valid: false},
		LicenseState:  pgtype.Text{String: "CA", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "Test", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Rollback
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}

	// Verify hospital was not persisted
	queries = db.New(tdb.pool)
	_, err = queries.GetHospitalByName(ctx, "Rollback Test Hospital")
	if err == nil {
		t.Error("Expected error getting rolled back hospital, got nil")
	}
}

func TestTransactionCommit(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()

	// Start transaction
	tx, err := tdb.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	queries := db.New(tx)

	// Insert hospital in transaction
	date, _ := time.Parse("2006-01-02", "2025-01-15")
	_, err = queries.InsertHospital(ctx, db.InsertHospitalParams{
		Name:          "Commit Test Hospital",
		Addresses:     []string{"123 Main St"},
		LocationNames: []string{"Main"},
		Npis:          []string{"1234567890"},
		LicenseNumber: pgtype.Text{Valid: false},
		LicenseState:  pgtype.Text{String: "CA", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "Test", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Commit
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify hospital was persisted
	queries = db.New(tdb.pool)
	hospital, err := queries.GetHospitalByName(ctx, "Commit Test Hospital")
	if err != nil {
		t.Fatalf("Expected to find committed hospital: %v", err)
	}
	if hospital.Name != "Commit Test Hospital" {
		t.Errorf("Expected name 'Commit Test Hospital', got '%s'", hospital.Name)
	}
}

func TestBatchInsert(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	ctx := context.Background()
	queries := db.New(tdb.pool)

	hospitalID := insertTestHospital(t, queries)

	// Insert many items in a transaction (simulating batch)
	tx, _ := tdb.pool.Begin(ctx)
	txQueries := db.New(tx)

	for i := 0; i < 100; i++ {
		_, err := txQueries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
			HospitalID:  hospitalID,
			Description: fmt.Sprintf("Test Item %d", i),
		})
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("Failed to insert item %d: %v", i, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit batch: %v", err)
	}

	// Verify all items were inserted
	var count int
	err := tdb.pool.QueryRow(ctx, "SELECT COUNT(*) FROM standard_charge_items WHERE hospital_id = $1", hospitalID).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count items: %v", err)
	}
	if count != 100 {
		t.Errorf("Expected 100 items, got %d", count)
	}
}

// Helper functions

func insertTestHospital(t *testing.T, queries *db.Queries) int32 {
	t.Helper()
	ctx := context.Background()
	date, _ := time.Parse("2006-01-02", "2025-01-15")

	hospitalID, err := queries.InsertHospital(ctx, db.InsertHospitalParams{
		Name:          "Test Hospital",
		Addresses:     []string{"123 Main St"},
		LocationNames: []string{"Main"},
		Npis:          []string{"1234567890"},
		LicenseNumber: pgtype.Text{Valid: false},
		LicenseState:  pgtype.Text{String: "CA", Valid: true},
		Version:       "3.0.0",
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{String: "Test", Valid: true},
	})
	if err != nil {
		t.Fatalf("Failed to insert test hospital: %v", err)
	}
	return hospitalID
}

func ptrFloat(f float64) *float64 {
	return &f
}
