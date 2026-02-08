package main

import (
	"context"
	_ "embed"
	"os"
	"path/filepath"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed testdata/schema.sql
var testSchema string

const testConnStr = "postgres://test:test@localhost:15433/test?sslmode=disable"

type testDB struct {
	pg   *embeddedpostgres.EmbeddedPostgres
	pool *pgxpool.Pool
}

func setupTestDB(t *testing.T) *testDB {
	t.Helper()

	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Username("test").
		Password("test").
		Database("test").
		Port(15433).
		StartTimeout(60 * time.Second))

	if err := pg.Start(); err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testConnStr)
	if err != nil {
		pg.Stop()
		t.Fatalf("connect: %v", err)
	}

	if _, err := pool.Exec(ctx, testSchema); err != nil {
		pool.Close()
		pg.Stop()
		t.Fatalf("init schema: %v", err)
	}

	return &testDB{pg: pg, pool: pool}
}

func (tdb *testDB) teardown() {
	if tdb.pool != nil {
		tdb.pool.Close()
	}
	if tdb.pg != nil {
		tdb.pg.Stop()
	}
}

// strPtr returns a pointer to s.
func strPtr(s string) *string { return &s }

// f64Ptr returns a pointer to f.
func f64Ptr(f float64) *float64 { return &f }

// writeTestParquet creates a small parquet file with known data and returns its path.
func writeTestParquet(t *testing.T) (string, []HospitalChargeRow) {
	t.Helper()

	rows := []HospitalChargeRow{
		// Item 1: two payers sharing same setting/gross/min/max (one charge group, two payer rows)
		{
			Description:    "ECHOCARDIOGRAM COMPLETE",
			Setting:        "outpatient",
			CPTCode:        strPtr("93306"),
			HCPCSCode:      strPtr("G0389"),
			GrossCharge:    f64Ptr(1500.00),
			DiscountedCash: f64Ptr(750.00),
			MinCharge:      f64Ptr(500.00),
			MaxCharge:      f64Ptr(2000.00),
			PayerName:      strPtr("Aetna"),
			PlanName:       strPtr("Aetna PPO"),
			NegotiatedDollar: f64Ptr(900.00),
			Methodology:    strPtr("fee_schedule"),
			HospitalName:   "Test General Hospital",
			LastUpdatedOn:   "2024-01-15",
			Version:        "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress: "123 Main St, New York, NY 10001",
			LicenseNumber:  strPtr("LIC-12345"),
			LicenseState:   strPtr("NY"),
			Affirmation:    true,
		},
		{
			Description:    "ECHOCARDIOGRAM COMPLETE",
			Setting:        "outpatient",
			CPTCode:        strPtr("93306"),
			HCPCSCode:      strPtr("G0389"),
			GrossCharge:    f64Ptr(1500.00),
			DiscountedCash: f64Ptr(750.00),
			MinCharge:      f64Ptr(500.00),
			MaxCharge:      f64Ptr(2000.00),
			PayerName:      strPtr("UnitedHealthcare"),
			PlanName:       strPtr("UHC Choice Plus"),
			NegotiatedDollar: f64Ptr(1100.00),
			Methodology:    strPtr("case_rate"),
			HospitalName:   "Test General Hospital",
			LastUpdatedOn:   "2024-01-15",
			Version:        "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress: "123 Main St, New York, NY 10001",
			LicenseNumber:  strPtr("LIC-12345"),
			LicenseState:   strPtr("NY"),
			Affirmation:    true,
		},
		// Item 2: different description/code, inpatient, single payer, with drug info
		{
			Description:           "ACETAMINOPHEN 500MG TABLET",
			Setting:               "inpatient",
			NDCCode:               strPtr("00456-0422-01"),
			GrossCharge:           f64Ptr(15.50),
			DiscountedCash:        f64Ptr(8.25),
			MinCharge:             f64Ptr(5.00),
			MaxCharge:             f64Ptr(20.00),
			PayerName:             strPtr("Cigna"),
			PlanName:              strPtr("Cigna Open Access"),
			NegotiatedDollar:      f64Ptr(10.00),
			NegotiatedPercentage:  f64Ptr(65.0),
			Methodology:           strPtr("fee_schedule"),
			DrugUnitOfMeasurement: f64Ptr(500.0),
			DrugTypeOfMeasurement: strPtr("ME"),
			AdditionalGenericNotes: strPtr("Oral tablet only"),
			AdditionalPayerNotes:  strPtr("Prior auth required"),
			HospitalName:          "Test General Hospital",
			LastUpdatedOn:          "2024-01-15",
			Version:               "2.0.0",
			HospitalLocation:      "New York, NY",
			HospitalAddress:       "123 Main St, New York, NY 10001",
			LicenseNumber:         strPtr("LIC-12345"),
			LicenseState:          strPtr("NY"),
			Affirmation:           true,
		},
		// Item 3: MS-DRG code, no payer (gross/discounted only)
		{
			Description:    "HEART TRANSPLANT WITH MCC",
			Setting:        "inpatient",
			MSDRGCode:      strPtr("001"),
			GrossCharge:    f64Ptr(500000.00),
			DiscountedCash: f64Ptr(250000.00),
			MinCharge:      f64Ptr(200000.00),
			MaxCharge:      f64Ptr(750000.00),
			HospitalName:   "Test General Hospital",
			LastUpdatedOn:   "2024-01-15",
			Version:        "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress: "123 Main St, New York, NY 10001",
			LicenseNumber:  strPtr("LIC-12345"),
			LicenseState:   strPtr("NY"),
			Affirmation:    true,
		},
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test_charges.parquet")

	w, err := NewChargeWriter(path)
	if err != nil {
		t.Fatalf("create writer: %v", err)
	}
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	return path, rows
}

func TestLoadParquetToPg(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	parquetPath, srcRows := writeTestParquet(t)
	defer os.Remove(parquetPath)

	ctx := context.Background()

	err := loadParquetToPg(ctx, parquetPath, testConnStr, 100)
	if err != nil {
		t.Fatalf("loadParquetToPg: %v", err)
	}

	// ── Verify hospital ────────────────────────────────────────────
	var hospitalName, version, address string
	err = tdb.pool.QueryRow(ctx,
		`SELECT name, version, addresses[1] FROM hospitals LIMIT 1`).
		Scan(&hospitalName, &version, &address)
	if err != nil {
		t.Fatalf("query hospital: %v", err)
	}
	if hospitalName != srcRows[0].HospitalName {
		t.Errorf("hospital name = %q, want %q", hospitalName, srcRows[0].HospitalName)
	}
	if version != srcRows[0].Version {
		t.Errorf("version = %q, want %q", version, srcRows[0].Version)
	}
	if address != srcRows[0].HospitalAddress {
		t.Errorf("address = %q, want %q", address, srcRows[0].HospitalAddress)
	}

	// ── Verify standard_charge_items count ─────────────────────────
	// 3 distinct items: ECHOCARDIOGRAM, ACETAMINOPHEN, HEART TRANSPLANT
	var itemCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM standard_charge_items`).Scan(&itemCount)
	if err != nil {
		t.Fatalf("count items: %v", err)
	}
	if itemCount != 3 {
		t.Errorf("items = %d, want 3", itemCount)
	}

	// ── Verify item descriptions ───────────────────────────────────
	var descriptions []string
	rows, err := tdb.pool.Query(ctx,
		`SELECT description FROM standard_charge_items ORDER BY description`)
	if err != nil {
		t.Fatalf("query descriptions: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			t.Fatalf("scan description: %v", err)
		}
		descriptions = append(descriptions, d)
	}
	wantDescriptions := []string{
		"ACETAMINOPHEN 500MG TABLET",
		"ECHOCARDIOGRAM COMPLETE",
		"HEART TRANSPLANT WITH MCC",
	}
	if len(descriptions) != len(wantDescriptions) {
		t.Fatalf("descriptions = %v, want %v", descriptions, wantDescriptions)
	}
	for i := range descriptions {
		if descriptions[i] != wantDescriptions[i] {
			t.Errorf("description[%d] = %q, want %q", i, descriptions[i], wantDescriptions[i])
		}
	}

	// ── Verify codes ───────────────────────────────────────────────
	var codeCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM codes`).Scan(&codeCount)
	if err != nil {
		t.Fatalf("count codes: %v", err)
	}
	// CPT:93306, HCPCS:G0389, NDC:00456-0422-01, MS-DRG:001 = 4 codes
	if codeCount != 4 {
		t.Errorf("codes = %d, want 4", codeCount)
	}

	// Verify specific codes exist
	type codeRow struct {
		code     string
		codeType string
	}
	wantCodes := []codeRow{
		{"93306", "CPT"},
		{"G0389", "HCPCS"},
		{"00456-0422-01", "NDC"},
		{"001", "MS-DRG"},
	}
	for _, wc := range wantCodes {
		var exists bool
		err = tdb.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM codes WHERE code = $1 AND code_type = $2)`,
			wc.code, wc.codeType).Scan(&exists)
		if err != nil {
			t.Fatalf("check code %s/%s: %v", wc.code, wc.codeType, err)
		}
		if !exists {
			t.Errorf("code %s/%s not found", wc.code, wc.codeType)
		}
	}

	// ── Verify item_codes linkage ──────────────────────────────────
	var itemCodeCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM item_codes`).Scan(&itemCodeCount)
	if err != nil {
		t.Fatalf("count item_codes: %v", err)
	}
	// ECHOCARDIOGRAM → CPT + HCPCS = 2, ACETAMINOPHEN → NDC = 1, HEART → MS-DRG = 1 = 4
	if itemCodeCount != 4 {
		t.Errorf("item_codes = %d, want 4", itemCodeCount)
	}

	// ── Verify standard_charges ────────────────────────────────────
	var chargeCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM standard_charges`).Scan(&chargeCount)
	if err != nil {
		t.Fatalf("count charges: %v", err)
	}
	// ECHOCARDIOGRAM: 1 charge group (both payers share same setting/gross/min/max)
	// ACETAMINOPHEN: 1 charge group
	// HEART TRANSPLANT: 1 charge group
	// = 3 charges
	if chargeCount != 3 {
		t.Errorf("charges = %d, want 3", chargeCount)
	}

	// Verify gross_charge values
	type chargeVal struct {
		description string
		gross       float64
	}
	var chargeVals []chargeVal
	crows, err := tdb.pool.Query(ctx,
		`SELECT sci.description, sc.gross_charge
		 FROM standard_charges sc
		 JOIN standard_charge_items sci ON sci.id = sc.item_id
		 ORDER BY sci.description`)
	if err != nil {
		t.Fatalf("query charge values: %v", err)
	}
	defer crows.Close()
	for crows.Next() {
		var cv chargeVal
		if err := crows.Scan(&cv.description, &cv.gross); err != nil {
			t.Fatalf("scan charge: %v", err)
		}
		chargeVals = append(chargeVals, cv)
	}
	wantChargeVals := []chargeVal{
		{"ACETAMINOPHEN 500MG TABLET", 15.50},
		{"ECHOCARDIOGRAM COMPLETE", 1500.00},
		{"HEART TRANSPLANT WITH MCC", 500000.00},
	}
	if len(chargeVals) != len(wantChargeVals) {
		t.Fatalf("charge vals = %v, want %v", chargeVals, wantChargeVals)
	}
	for i := range chargeVals {
		if chargeVals[i].description != wantChargeVals[i].description {
			t.Errorf("charge[%d].description = %q, want %q", i, chargeVals[i].description, wantChargeVals[i].description)
		}
		if chargeVals[i].gross != wantChargeVals[i].gross {
			t.Errorf("charge[%d].gross = %f, want %f", i, chargeVals[i].gross, wantChargeVals[i].gross)
		}
	}

	// ── Verify payer_charges ───────────────────────────────────────
	var payerChargeCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM payer_charges`).Scan(&payerChargeCount)
	if err != nil {
		t.Fatalf("count payer_charges: %v", err)
	}
	// ECHOCARDIOGRAM: 2 payers, ACETAMINOPHEN: 1 payer, HEART: 0 payers = 3
	if payerChargeCount != 3 {
		t.Errorf("payer_charges = %d, want 3", payerChargeCount)
	}

	// Verify payer details
	type payerDetail struct {
		payerName  string
		planName   string
		dollar     float64
		methodology string
	}
	var payers []payerDetail
	prows, err := tdb.pool.Query(ctx,
		`SELECT pc.payer_name, p.name, pc.standard_charge_dollar, pc.methodology
		 FROM payer_charges pc
		 JOIN plans p ON p.id = pc.plan_id
		 ORDER BY pc.payer_name`)
	if err != nil {
		t.Fatalf("query payer details: %v", err)
	}
	defer prows.Close()
	for prows.Next() {
		var pd payerDetail
		if err := prows.Scan(&pd.payerName, &pd.planName, &pd.dollar, &pd.methodology); err != nil {
			t.Fatalf("scan payer: %v", err)
		}
		payers = append(payers, pd)
	}
	wantPayers := []payerDetail{
		{"Aetna", "Aetna PPO", 900.00, "fee_schedule"},
		{"Cigna", "Cigna Open Access", 10.00, "fee_schedule"},
		{"UnitedHealthcare", "UHC Choice Plus", 1100.00, "case_rate"},
	}
	if len(payers) != len(wantPayers) {
		t.Fatalf("payers = %v, want %v", payers, wantPayers)
	}
	for i := range payers {
		if payers[i] != wantPayers[i] {
			t.Errorf("payer[%d] = %+v, want %+v", i, payers[i], wantPayers[i])
		}
	}

	// ── Verify plans ───────────────────────────────────────────────
	var planCount int
	err = tdb.pool.QueryRow(ctx, `SELECT count(*) FROM plans`).Scan(&planCount)
	if err != nil {
		t.Fatalf("count plans: %v", err)
	}
	// Aetna PPO, UHC Choice Plus, Cigna Open Access = 3
	if planCount != 3 {
		t.Errorf("plans = %d, want 3", planCount)
	}

	// ── Verify drug info on ACETAMINOPHEN item ─────────────────────
	var drugUnit float64
	var drugUnitType string
	err = tdb.pool.QueryRow(ctx,
		`SELECT drug_unit, drug_unit_type FROM standard_charge_items
		 WHERE description = 'ACETAMINOPHEN 500MG TABLET'`).
		Scan(&drugUnit, &drugUnitType)
	if err != nil {
		t.Fatalf("query drug info: %v", err)
	}
	if drugUnit != 500.0 {
		t.Errorf("drug_unit = %f, want 500.0", drugUnit)
	}
	if drugUnitType != "ME" {
		t.Errorf("drug_unit_type = %q, want %q", drugUnitType, "ME")
	}

	// ── Verify additional notes ────────────────────────────────────
	var genericNotes, payerNotes string
	err = tdb.pool.QueryRow(ctx,
		`SELECT sc.additional_notes, pc.additional_notes
		 FROM standard_charges sc
		 JOIN standard_charge_items sci ON sci.id = sc.item_id
		 JOIN payer_charges pc ON pc.standard_charge_id = sc.id
		 WHERE sci.description = 'ACETAMINOPHEN 500MG TABLET'`).
		Scan(&genericNotes, &payerNotes)
	if err != nil {
		t.Fatalf("query notes: %v", err)
	}
	if genericNotes != "Oral tablet only" {
		t.Errorf("generic_notes = %q, want %q", genericNotes, "Oral tablet only")
	}
	if payerNotes != "Prior auth required" {
		t.Errorf("payer_notes = %q, want %q", payerNotes, "Prior auth required")
	}
}
