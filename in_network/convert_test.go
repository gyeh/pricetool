package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const examplesDir = "schemas/examples"

func convertTestFile(t *testing.T, name string) ([]RateRow, []ProviderRow) {
	t.Helper()

	inputPath := filepath.Join(examplesDir, name)
	f, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("open %s: %v", inputPath, err)
	}
	defer f.Close()

	tmpDir := t.TempDir()
	ratesPath := filepath.Join(tmpDir, "rates.parquet")
	providersPath := filepath.Join(tmpDir, "providers.parquet")

	rw, err := NewRateParquetWriter(ratesPath)
	if err != nil {
		t.Fatalf("rate writer: %v", err)
	}

	pw, err := NewProviderParquetWriter(providersPath)
	if err != nil {
		rw.Close()
		t.Fatalf("provider writer: %v", err)
	}

	converter := NewStreamConverter(f, false)
	_, err = converter.Convert(rw, pw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	rw.Close()
	pw.Close()

	rates := readRateRows(t, ratesPath)
	providers := readProviderRows(t, providersPath)
	return rates, providers
}

func readRateRows(t *testing.T, path string) []RateRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	reader := parquet.NewGenericReader[RateRow](pf)
	defer reader.Close()

	rows := make([]RateRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	return rows[:n]
}

func readProviderRows(t *testing.T, path string) []ProviderRow {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	pf, err := parquet.OpenFile(f, stat.Size())
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}

	reader := parquet.NewGenericReader[ProviderRow](pf)
	defer reader.Close()

	rows := make([]ProviderRow, reader.NumRows())
	n, err := reader.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read: %v", err)
	}
	return rows[:n]
}

func TestFeeForServiceSinglePlan(t *testing.T) {
	rates, providers := convertTestFile(t, "in-network-rates-fee-for-service-single-plan-sample.json")

	if len(rates) != 5 {
		t.Fatalf("expected 5 rate rows, got %d", len(rates))
	}
	if len(providers) != 15 {
		t.Fatalf("expected 15 provider rows, got %d", len(providers))
	}

	// Check metadata on first row
	r := rates[0]
	if r.ReportingEntityName != "medicare" {
		t.Errorf("reporting_entity_name = %q, want medicare", r.ReportingEntityName)
	}
	if r.PlanName == nil || *r.PlanName != "Plan A PPO" {
		t.Errorf("plan_name = %v, want Plan A PPO", r.PlanName)
	}
	if r.PlanMarketType == nil || *r.PlanMarketType != "group" {
		t.Errorf("plan_market_type = %v, want group", r.PlanMarketType)
	}
	if r.Version != "2.0.0" {
		t.Errorf("version = %q, want 2.0.0", r.Version)
	}
	if r.PlanSponsorName == nil || *r.PlanSponsorName != "ACME small auto shop" {
		t.Errorf("plan_sponsor_name = %v, want ACME small auto shop", r.PlanSponsorName)
	}

	// First price: 27447, professional, 123.45, modifier AS, service_code [18,19,11]
	if r.BillingCode != "27447" {
		t.Errorf("billing_code = %q, want 27447", r.BillingCode)
	}
	if r.NegotiatedRate != 123.45 {
		t.Errorf("negotiated_rate = %v, want 123.45", r.NegotiatedRate)
	}
	if r.BillingClass != "professional" {
		t.Errorf("billing_class = %q, want professional", r.BillingClass)
	}
	if len(r.BillingCodeModifier) != 1 || r.BillingCodeModifier[0] != "AS" {
		t.Errorf("billing_code_modifier = %v, want [AS]", r.BillingCodeModifier)
	}
	if len(r.ServiceCode) != 3 {
		t.Errorf("service_code len = %d, want 3", len(r.ServiceCode))
	}
	if r.Setting != "inpatient" {
		t.Errorf("setting = %q, want inpatient", r.Setting)
	}
	if r.ExpirationDate != "2022-01-01" {
		t.Errorf("expiration_date = %q, want 2022-01-01", r.ExpirationDate)
	}

	// Check provider_group_ids
	if len(r.ProviderGroupIDs) != 1 || r.ProviderGroupIDs[0] != 1 {
		t.Errorf("provider_group_ids = %v, want [1]", r.ProviderGroupIDs)
	}

	// Second price: 27447, institutional, 1230.45
	r2 := rates[1]
	if r2.NegotiatedRate != 1230.45 {
		t.Errorf("rate[1] negotiated_rate = %v, want 1230.45", r2.NegotiatedRate)
	}
	if r2.BillingClass != "institutional" {
		t.Errorf("rate[1] billing_class = %q, want institutional", r2.BillingClass)
	}

	// Check provider rows
	p := providers[0]
	if p.ProviderGroupID != 1 {
		t.Errorf("provider_group_id = %d, want 1", p.ProviderGroupID)
	}
	if p.TINType != "ein" {
		t.Errorf("tin_type = %q, want ein", p.TINType)
	}
	if p.BusinessName == nil || *p.BusinessName != "ACME Provider Group" {
		t.Errorf("business_name = %v, want ACME Provider Group", p.BusinessName)
	}
}

func TestBundleSinglePlan(t *testing.T) {
	rates, _ := convertTestFile(t, "in-network-rates-bundle-single-plan-sample.json")

	if len(rates) != 2 {
		t.Fatalf("expected 2 rate rows, got %d", len(rates))
	}

	r := rates[0]
	if r.NegotiationArrangement != "bundle" {
		t.Errorf("negotiation_arrangement = %q, want bundle", r.NegotiationArrangement)
	}
	if r.BillingCodeType != "ICD" {
		t.Errorf("billing_code_type = %q, want ICD", r.BillingCodeType)
	}
	if r.BundledCodesJSON == nil {
		t.Fatal("bundled_codes_json should not be nil")
	}
	if !strings.Contains(*r.BundledCodesJSON, "27447") {
		t.Errorf("bundled_codes_json should contain 27447, got %s", *r.BundledCodesJSON)
	}
	if !strings.Contains(*r.BundledCodesJSON, "27446") {
		t.Errorf("bundled_codes_json should contain 27446, got %s", *r.BundledCodesJSON)
	}
	if r.CoveredServicesJSON != nil {
		t.Errorf("covered_services_json should be nil for bundle, got %s", *r.CoveredServicesJSON)
	}
}

func TestCapitationSinglePlan(t *testing.T) {
	rates, _ := convertTestFile(t, "in-network-rates-capitation-single-plan-sample.json")

	if len(rates) != 2 {
		t.Fatalf("expected 2 rate rows, got %d", len(rates))
	}

	r := rates[0]
	if r.NegotiationArrangement != "capitation" {
		t.Errorf("negotiation_arrangement = %q, want capitation", r.NegotiationArrangement)
	}
	if r.CoveredServicesJSON == nil {
		t.Fatal("covered_services_json should not be nil")
	}
	if !strings.Contains(*r.CoveredServicesJSON, "27447") {
		t.Errorf("covered_services_json should contain 27447, got %s", *r.CoveredServicesJSON)
	}
	if r.BundledCodesJSON != nil {
		t.Errorf("bundled_codes_json should be nil for capitation, got %s", *r.BundledCodesJSON)
	}
}

func TestMultiplePlans(t *testing.T) {
	rates, _ := convertTestFile(t, "in-network-rates-multiple-plans-sample.json")

	if len(rates) != 6 {
		t.Fatalf("expected 6 rate rows, got %d", len(rates))
	}

	// plan_name should be nil (multi-plan file has no plan_name)
	if rates[0].PlanName != nil {
		t.Errorf("plan_name should be nil, got %q", *rates[0].PlanName)
	}
	if rates[0].IssuerName != nil {
		t.Errorf("issuer_name should be nil, got %q", *rates[0].IssuerName)
	}

	// Check derived type exists
	foundDerived := false
	for _, r := range rates {
		if r.NegotiatedType == "derived" {
			foundDerived = true
			break
		}
	}
	if !foundDerived {
		t.Error("expected at least one derived negotiated_type")
	}

	// Check "both" billing_class
	foundBoth := false
	for _, r := range rates {
		if r.BillingClass == "both" {
			foundBoth = true
			break
		}
	}
	if !foundBoth {
		t.Error("expected at least one 'both' billing_class")
	}
}

func TestAllNegotiatedTypes(t *testing.T) {
	rates, providers := convertTestFile(t, "in-network-rates-all-negotiated-types-sample.json")

	if len(rates) != 8 {
		t.Fatalf("expected 8 rate rows, got %d", len(rates))
	}

	// Verify all 5 negotiated types are present
	typesSeen := make(map[string]bool)
	for _, r := range rates {
		typesSeen[r.NegotiatedType] = true
	}

	expected := []string{"negotiated", "percentage", "per diem", "derived", "fee schedule"}
	for _, typ := range expected {
		if !typesSeen[typ] {
			t.Errorf("missing negotiated_type %q, seen: %v", typ, typesSeen)
		}
	}

	// Check plan metadata
	if rates[0].PlanName == nil || *rates[0].PlanName != "Plan D PPO" {
		t.Errorf("plan_name = %v, want Plan D PPO", rates[0].PlanName)
	}

	// Check additional_information present on some rows
	foundAddlInfo := false
	for _, r := range rates {
		if r.AdditionalInformation != nil {
			foundAddlInfo = true
			break
		}
	}
	if !foundAddlInfo {
		t.Error("expected at least one row with additional_information")
	}

	// Check provider rows: 2 refs, 4 + 2 NPIs across groups
	if len(providers) != 6 {
		t.Fatalf("expected 6 provider rows, got %d", len(providers))
	}

	// Check multi-provider ref: item 5 (27447) has provider_references [1,2]
	// Find the knee replacement rate rows
	for _, r := range rates {
		if r.BillingCode == "27447" && r.NegotiatedType == "fee schedule" {
			if len(r.ProviderGroupIDs) != 2 {
				t.Errorf("27447 fee schedule provider_group_ids = %v, want 2 elements", r.ProviderGroupIDs)
			}
			break
		}
	}
}

func convertTestFileWithNPIFilter(t *testing.T, name string, npiFilter map[int64]bool) ([]RateRow, []ProviderRow) {
	t.Helper()

	inputPath := filepath.Join(examplesDir, name)
	f, err := os.Open(inputPath)
	if err != nil {
		t.Fatalf("open %s: %v", inputPath, err)
	}
	defer f.Close()

	tmpDir := t.TempDir()
	ratesPath := filepath.Join(tmpDir, "rates.parquet")
	providersPath := filepath.Join(tmpDir, "providers.parquet")

	rw, err := NewRateParquetWriter(ratesPath)
	if err != nil {
		t.Fatalf("rate writer: %v", err)
	}

	pw, err := NewProviderParquetWriter(providersPath)
	if err != nil {
		rw.Close()
		t.Fatalf("provider writer: %v", err)
	}

	converter := NewStreamConverter(f, false)
	converter.SetNPIFilter(npiFilter)
	_, err = converter.Convert(rw, pw)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	rw.Close()
	pw.Close()

	rates := readRateRows(t, ratesPath)
	providers := readProviderRows(t, providersPath)
	return rates, providers
}

func TestNPIFilterExcludesAll(t *testing.T) {
	filter := map[int64]bool{9999999999: true}
	rates, providers := convertTestFileWithNPIFilter(t, "in-network-rates-all-negotiated-types-sample.json", filter)

	if len(rates) != 0 {
		t.Errorf("expected 0 rate rows, got %d", len(rates))
	}
	if len(providers) != 0 {
		t.Errorf("expected 0 provider rows, got %d", len(providers))
	}
}

func TestNPIFilterPartialMatch(t *testing.T) {
	// Group 2 NPIs only: 5678901234, 6789012345
	filter := map[int64]bool{5678901234: true, 6789012345: true}
	rates, providers := convertTestFileWithNPIFilter(t, "in-network-rates-all-negotiated-types-sample.json", filter)

	// Provider rows: only the 2 NPIs from group 2
	if len(providers) != 2 {
		t.Fatalf("expected 2 provider rows, got %d", len(providers))
	}
	for _, p := range providers {
		if p.ProviderGroupID != 2 {
			t.Errorf("expected provider_group_id=2, got %d", p.ProviderGroupID)
		}
	}

	// Rate rows: items referencing group 2:
	//   0200 (group 2 only) → 1 price
	//   27447 (groups 1,2) → 2 prices (group 1 filtered out, group 2 remains)
	//   99285 (group 2 only) → 2 prices
	// Total: 5 rates
	if len(rates) != 5 {
		t.Fatalf("expected 5 rate rows, got %d", len(rates))
	}

	// 27447 rates should have provider_group_ids trimmed to [2]
	for _, r := range rates {
		if r.BillingCode == "27447" {
			if len(r.ProviderGroupIDs) != 1 || r.ProviderGroupIDs[0] != 2 {
				t.Errorf("27447 provider_group_ids = %v, want [2]", r.ProviderGroupIDs)
			}
		}
	}
}

func TestNPIFilterSingleNPI(t *testing.T) {
	// Single NPI from group 1
	filter := map[int64]bool{1234567890: true}
	rates, providers := convertTestFileWithNPIFilter(t, "in-network-rates-all-negotiated-types-sample.json", filter)

	// Provider rows: only 1 NPI
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider row, got %d", len(providers))
	}
	if providers[0].NPI != 1234567890 {
		t.Errorf("expected NPI 1234567890, got %d", providers[0].NPI)
	}

	// Rate rows: items referencing group 1:
	//   99214 (group 1) → 1 price
	//   97110 (group 1) → 1 price
	//   80053 (group 1) → 1 price
	//   27447 (groups 1,2) → 2 prices (group 2 filtered out, group 1 remains)
	// Total: 5 rates
	if len(rates) != 5 {
		t.Fatalf("expected 5 rate rows, got %d", len(rates))
	}

	// 27447 rates should have provider_group_ids trimmed to [1]
	for _, r := range rates {
		if r.BillingCode == "27447" {
			if len(r.ProviderGroupIDs) != 1 || r.ProviderGroupIDs[0] != 1 {
				t.Errorf("27447 provider_group_ids = %v, want [1]", r.ProviderGroupIDs)
			}
		}
	}
}

func TestNoNPI(t *testing.T) {
	rates, providers := convertTestFile(t, "in-network-rates-no-npi.json")

	if len(rates) != 1 {
		t.Fatalf("expected 1 rate row, got %d", len(rates))
	}
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider row, got %d", len(providers))
	}

	p := providers[0]
	if p.TINType != "npi" {
		t.Errorf("tin_type = %q, want npi", p.TINType)
	}
	if p.TINValue != "1234567890" {
		t.Errorf("tin_value = %q, want 1234567890", p.TINValue)
	}
	if p.BusinessName != nil {
		t.Errorf("business_name should be nil for npi TIN, got %q", *p.BusinessName)
	}
	if p.NPI != 1111111111 {
		t.Errorf("npi = %d, want 1111111111", p.NPI)
	}

	r := rates[0]
	if r.BillingClass != "institutional" {
		t.Errorf("billing_class = %q, want institutional", r.BillingClass)
	}
	if r.NegotiatedRate != 123.45 {
		t.Errorf("negotiated_rate = %v, want 123.45", r.NegotiatedRate)
	}
}
