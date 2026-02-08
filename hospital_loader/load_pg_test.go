package main

import (
	"context"
	_ "embed"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"hospital_loader/db"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5/pgtype"
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

// numericToFloat64 converts pgtype.Numeric to float64 for test comparison.
func numericToFloat64(t *testing.T, n pgtype.Numeric) float64 {
	t.Helper()
	if !n.Valid {
		t.Fatal("expected valid numeric, got NULL")
	}
	f, _ := new(big.Float).SetInt(n.Int).Float64()
	// Adjust for exponent
	for i := int32(0); i < -n.Exp; i++ {
		f /= 10
	}
	for i := int32(0); i < n.Exp; i++ {
		f *= 10
	}
	return f
}

// writeTestParquet creates a small parquet file with known data and returns its path.
func writeTestParquet(t *testing.T) (string, []HospitalChargeRow) {
	t.Helper()

	rows := []HospitalChargeRow{
		// Item 1: two payers sharing same setting/gross/min/max (one charge group, two payer rows)
		{
			Description:      "ECHOCARDIOGRAM COMPLETE",
			Setting:          "outpatient",
			CPTCode:          strPtr("93306"),
			HCPCSCode:        strPtr("G0389"),
			GrossCharge:      f64Ptr(1500.00),
			DiscountedCash:   f64Ptr(750.00),
			MinCharge:        f64Ptr(500.00),
			MaxCharge:        f64Ptr(2000.00),
			PayerName:        strPtr("Aetna"),
			PlanName:         strPtr("Aetna PPO"),
			NegotiatedDollar: f64Ptr(900.00),
			Methodology:      strPtr("fee_schedule"),
			HospitalName:     "Test General Hospital",
			LastUpdatedOn:    "2024-01-15",
			Version:          "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress:  "123 Main St, New York, NY 10001",
			LicenseNumber:    strPtr("LIC-12345"),
			LicenseState:     strPtr("NY"),
			Affirmation:      true,
		},
		{
			Description:      "ECHOCARDIOGRAM COMPLETE",
			Setting:          "outpatient",
			CPTCode:          strPtr("93306"),
			HCPCSCode:        strPtr("G0389"),
			GrossCharge:      f64Ptr(1500.00),
			DiscountedCash:   f64Ptr(750.00),
			MinCharge:        f64Ptr(500.00),
			MaxCharge:        f64Ptr(2000.00),
			PayerName:        strPtr("UnitedHealthcare"),
			PlanName:         strPtr("UHC Choice Plus"),
			NegotiatedDollar: f64Ptr(1100.00),
			Methodology:      strPtr("case_rate"),
			HospitalName:     "Test General Hospital",
			LastUpdatedOn:    "2024-01-15",
			Version:          "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress:  "123 Main St, New York, NY 10001",
			LicenseNumber:    strPtr("LIC-12345"),
			LicenseState:     strPtr("NY"),
			Affirmation:      true,
		},
		// Item 2: different description/code, inpatient, single payer, with drug info
		{
			Description:            "ACETAMINOPHEN 500MG TABLET",
			Setting:                "inpatient",
			NDCCode:                strPtr("00456-0422-01"),
			GrossCharge:            f64Ptr(15.50),
			DiscountedCash:         f64Ptr(8.25),
			MinCharge:              f64Ptr(5.00),
			MaxCharge:              f64Ptr(20.00),
			PayerName:              strPtr("Cigna"),
			PlanName:               strPtr("Cigna Open Access"),
			NegotiatedDollar:       f64Ptr(10.00),
			NegotiatedPercentage:   f64Ptr(65.0),
			Methodology:            strPtr("fee_schedule"),
			DrugUnitOfMeasurement:  f64Ptr(500.0),
			DrugTypeOfMeasurement:  strPtr("ME"),
			AdditionalGenericNotes: strPtr("Oral tablet only"),
			AdditionalPayerNotes:   strPtr("Prior auth required"),
			HospitalName:           "Test General Hospital",
			LastUpdatedOn:          "2024-01-15",
			Version:                "2.0.0",
			HospitalLocation:       "New York, NY",
			HospitalAddress:        "123 Main St, New York, NY 10001",
			LicenseNumber:          strPtr("LIC-12345"),
			LicenseState:           strPtr("NY"),
			Affirmation:            true,
		},
		// Item 3: MS-DRG code, no payer (gross/discounted only)
		{
			Description:      "HEART TRANSPLANT WITH MCC",
			Setting:          "inpatient",
			MSDRGCode:        strPtr("001"),
			GrossCharge:      f64Ptr(500000.00),
			DiscountedCash:   f64Ptr(250000.00),
			MinCharge:        f64Ptr(200000.00),
			MaxCharge:        f64Ptr(750000.00),
			HospitalName:     "Test General Hospital",
			LastUpdatedOn:    "2024-01-15",
			Version:          "2.0.0",
			HospitalLocation: "New York, NY",
			HospitalAddress:  "123 Main St, New York, NY 10001",
			LicenseNumber:    strPtr("LIC-12345"),
			LicenseState:     strPtr("NY"),
			Affirmation:      true,
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

	err := loadParquetToPg(ctx, parquetPath, testConnStr, 100, false)
	if err != nil {
		t.Fatalf("loadParquetToPg: %v", err)
	}

	q := db.New(tdb.pool)

	// ── Verify hospital ────────────────────────────────────────────
	hospital, err := q.GetFirstHospital(ctx)
	if err != nil {
		t.Fatalf("GetFirstHospital: %v", err)
	}
	if hospital.Name != srcRows[0].HospitalName {
		t.Errorf("hospital name = %q, want %q", hospital.Name, srcRows[0].HospitalName)
	}
	if hospital.Version != srcRows[0].Version {
		t.Errorf("version = %q, want %q", hospital.Version, srcRows[0].Version)
	}
	if hospital.FirstAddress != srcRows[0].HospitalAddress {
		t.Errorf("address = %q, want %q", hospital.FirstAddress, srcRows[0].HospitalAddress)
	}

	// ── Verify standard_charge_items count ─────────────────────────
	// 3 distinct items: ECHOCARDIOGRAM, ACETAMINOPHEN, HEART TRANSPLANT
	itemCount, err := q.CountItems(ctx)
	if err != nil {
		t.Fatalf("CountItems: %v", err)
	}
	if itemCount != 3 {
		t.Errorf("items = %d, want 3", itemCount)
	}

	// ── Verify item descriptions ───────────────────────────────────
	descriptions, err := q.ListItemDescriptions(ctx)
	if err != nil {
		t.Fatalf("ListItemDescriptions: %v", err)
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
	codeCount, err := q.CountCodes(ctx)
	if err != nil {
		t.Fatalf("CountCodes: %v", err)
	}
	// CPT:93306, HCPCS:G0389, NDC:00456-0422-01, MS-DRG:001 = 4 codes
	if codeCount != 4 {
		t.Errorf("codes = %d, want 4", codeCount)
	}

	// Verify specific codes exist
	wantCodes := []db.CodeExistsParams{
		{Code: "93306", CodeType: "CPT"},
		{Code: "G0389", CodeType: "HCPCS"},
		{Code: "00456-0422-01", CodeType: "NDC"},
		{Code: "001", CodeType: "MS-DRG"},
	}
	for _, wc := range wantCodes {
		exists, err := q.CodeExists(ctx, wc)
		if err != nil {
			t.Fatalf("CodeExists %s/%s: %v", wc.Code, wc.CodeType, err)
		}
		if !exists {
			t.Errorf("code %s/%s not found", wc.Code, wc.CodeType)
		}
	}

	// ── Verify item_codes linkage ──────────────────────────────────
	itemCodeCount, err := q.CountItemCodes(ctx)
	if err != nil {
		t.Fatalf("CountItemCodes: %v", err)
	}
	// ECHOCARDIOGRAM → CPT + HCPCS = 2, ACETAMINOPHEN → NDC = 1, HEART → MS-DRG = 1 = 4
	if itemCodeCount != 4 {
		t.Errorf("item_codes = %d, want 4", itemCodeCount)
	}

	// ── Verify standard_charges ────────────────────────────────────
	chargeCount, err := q.CountCharges(ctx)
	if err != nil {
		t.Fatalf("CountCharges: %v", err)
	}
	// ECHOCARDIOGRAM: 1 charge group (both payers share same setting/gross/min/max)
	// ACETAMINOPHEN: 1 charge group
	// HEART TRANSPLANT: 1 charge group
	// = 3 charges
	if chargeCount != 3 {
		t.Errorf("charges = %d, want 3", chargeCount)
	}

	// Verify gross_charge values
	chargeVals, err := q.ListChargeValues(ctx)
	if err != nil {
		t.Fatalf("ListChargeValues: %v", err)
	}
	type chargeVal struct {
		description string
		gross       float64
	}
	wantChargeVals := []chargeVal{
		{"ACETAMINOPHEN 500MG TABLET", 15.50},
		{"ECHOCARDIOGRAM COMPLETE", 1500.00},
		{"HEART TRANSPLANT WITH MCC", 500000.00},
	}
	if len(chargeVals) != len(wantChargeVals) {
		t.Fatalf("charge vals count = %d, want %d", len(chargeVals), len(wantChargeVals))
	}
	for i, cv := range chargeVals {
		if cv.Description != wantChargeVals[i].description {
			t.Errorf("charge[%d].description = %q, want %q", i, cv.Description, wantChargeVals[i].description)
		}
		gross := numericToFloat64(t, cv.GrossCharge)
		if gross != wantChargeVals[i].gross {
			t.Errorf("charge[%d].gross = %f, want %f", i, gross, wantChargeVals[i].gross)
		}
	}

	// ── Verify payer_charges ───────────────────────────────────────
	payerChargeCount, err := q.CountPayerCharges(ctx)
	if err != nil {
		t.Fatalf("CountPayerCharges: %v", err)
	}
	// ECHOCARDIOGRAM: 2 payers, ACETAMINOPHEN: 1 payer, HEART: 0 payers = 3
	if payerChargeCount != 3 {
		t.Errorf("payer_charges = %d, want 3", payerChargeCount)
	}

	// Verify payer details
	payers, err := q.ListPayerDetails(ctx)
	if err != nil {
		t.Fatalf("ListPayerDetails: %v", err)
	}
	type payerDetail struct {
		payerName   string
		planName    string
		dollar      float64
		methodology string
	}
	wantPayers := []payerDetail{
		{"Aetna", "Aetna PPO", 900.00, "fee_schedule"},
		{"Cigna", "Cigna Open Access", 10.00, "fee_schedule"},
		{"UnitedHealthcare", "UHC Choice Plus", 1100.00, "case_rate"},
	}
	if len(payers) != len(wantPayers) {
		t.Fatalf("payers count = %d, want %d", len(payers), len(wantPayers))
	}
	for i, p := range payers {
		if p.PayerName != wantPayers[i].payerName {
			t.Errorf("payer[%d].name = %q, want %q", i, p.PayerName, wantPayers[i].payerName)
		}
		if p.PlanName != wantPayers[i].planName {
			t.Errorf("payer[%d].plan = %q, want %q", i, p.PlanName, wantPayers[i].planName)
		}
		dollar := numericToFloat64(t, p.StandardChargeDollar)
		if dollar != wantPayers[i].dollar {
			t.Errorf("payer[%d].dollar = %f, want %f", i, dollar, wantPayers[i].dollar)
		}
		if p.Methodology != wantPayers[i].methodology {
			t.Errorf("payer[%d].methodology = %q, want %q", i, p.Methodology, wantPayers[i].methodology)
		}
	}

	// ── Verify plans ───────────────────────────────────────────────
	planCount, err := q.CountPlans(ctx)
	if err != nil {
		t.Fatalf("CountPlans: %v", err)
	}
	// Aetna PPO, UHC Choice Plus, Cigna Open Access = 3
	if planCount != 3 {
		t.Errorf("plans = %d, want 3", planCount)
	}

	// ── Verify payers ──────────────────────────────────────────────
	payerCount, err := q.CountPayers(ctx)
	if err != nil {
		t.Fatalf("CountPayers: %v", err)
	}
	// Aetna, UnitedHealthcare, Cigna = 3
	if payerCount != 3 {
		t.Errorf("payers = %d, want 3", payerCount)
	}

	// ── Verify drug info on ACETAMINOPHEN item ─────────────────────
	drugInfo, err := q.GetItemDrugInfo(ctx, "ACETAMINOPHEN 500MG TABLET")
	if err != nil {
		t.Fatalf("GetItemDrugInfo: %v", err)
	}
	drugUnit := numericToFloat64(t, drugInfo.DrugUnit)
	if drugUnit != 500.0 {
		t.Errorf("drug_unit = %f, want 500.0", drugUnit)
	}
	if !drugInfo.DrugUnitType.Valid || drugInfo.DrugUnitType.String != "ME" {
		t.Errorf("drug_unit_type = %v, want %q", drugInfo.DrugUnitType, "ME")
	}

	// ── Verify additional notes ────────────────────────────────────
	notes, err := q.GetItemNotes(ctx, "ACETAMINOPHEN 500MG TABLET")
	if err != nil {
		t.Fatalf("GetItemNotes: %v", err)
	}
	if !notes.GenericNotes.Valid || notes.GenericNotes.String != "Oral tablet only" {
		t.Errorf("generic_notes = %v, want %q", notes.GenericNotes, "Oral tablet only")
	}
	if !notes.PayerNotes.Valid || notes.PayerNotes.String != "Prior auth required" {
		t.Errorf("payer_notes = %v, want %q", notes.PayerNotes, "Prior auth required")
	}
}

func TestLoadParquetToPg_SkipPayerCharges(t *testing.T) {
	tdb := setupTestDB(t)
	defer tdb.teardown()

	parquetPath, _ := writeTestParquet(t)
	defer os.Remove(parquetPath)

	ctx := context.Background()

	err := loadParquetToPg(ctx, parquetPath, testConnStr, 100, true)
	if err != nil {
		t.Fatalf("loadParquetToPg: %v", err)
	}

	q := db.New(tdb.pool)

	// Items should still be created
	itemCount, err := q.CountItems(ctx)
	if err != nil {
		t.Fatalf("CountItems: %v", err)
	}
	if itemCount != 3 {
		t.Errorf("items = %d, want 3", itemCount)
	}

	// Charges should still be created
	chargeCount, err := q.CountCharges(ctx)
	if err != nil {
		t.Fatalf("CountCharges: %v", err)
	}
	if chargeCount != 3 {
		t.Errorf("charges = %d, want 3", chargeCount)
	}

	// Codes should still be created
	codeCount, err := q.CountCodes(ctx)
	if err != nil {
		t.Fatalf("CountCodes: %v", err)
	}
	if codeCount != 4 {
		t.Errorf("codes = %d, want 4", codeCount)
	}

	// Payer charges should be empty
	payerChargeCount, err := q.CountPayerCharges(ctx)
	if err != nil {
		t.Fatalf("CountPayerCharges: %v", err)
	}
	if payerChargeCount != 0 {
		t.Errorf("payer_charges = %d, want 0", payerChargeCount)
	}

	// Payers table should be empty
	payerCount, err := q.CountPayers(ctx)
	if err != nil {
		t.Fatalf("CountPayers: %v", err)
	}
	if payerCount != 0 {
		t.Errorf("payers = %d, want 0", payerCount)
	}

	// Plans table should be empty
	planCount, err := q.CountPlans(ctx)
	if err != nil {
		t.Fatalf("CountPlans: %v", err)
	}
	if planCount != 0 {
		t.Errorf("plans = %d, want 0", planCount)
	}
}
