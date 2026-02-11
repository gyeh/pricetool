package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestParquetWriter(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test_*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	// Create writer
	writer, err := NewParquetWriter(tmpPath)
	if err != nil {
		t.Fatalf("Failed to create parquet writer: %v", err)
	}

	// Write test plans
	testPlans := []NYSPlanOutput{
		{
			PlanName:       "Test Plan 1",
			PlanIDType:     "hios",
			PlanID:         "12345",
			PlanMarketType: "individual",
			IssuerName:     "Test Issuer",
			Description:    "A test plan",
			InNetworkURLs:  []string{"https://example.com/file1.json", "https://example.com/file2.json"},
		},
		{
			PlanName:       "Test Plan 2",
			PlanIDType:     "ein",
			PlanID:         "987654321",
			PlanMarketType: "group",
			IssuerName:     "Another Issuer",
			Description:    "Another test plan",
			InNetworkURLs:  []string{"https://example.com/file3.json"},
		},
	}

	for _, plan := range testPlans {
		if err := writer.Write(plan); err != nil {
			t.Fatalf("Failed to write plan: %v", err)
		}
	}

	if writer.Count() != 2 {
		t.Errorf("Expected count 2, got %d", writer.Count())
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify file was created and has content
	fileInfo, err := os.Stat(tmpPath)
	if err != nil {
		t.Fatalf("Failed to stat output file: %v", err)
	}
	if fileInfo.Size() == 0 {
		t.Error("Parquet file is empty")
	}

	// Read back and verify using ReadFile helper
	records, err := parquet.ReadFile[NYSPlanParquet](tmpPath)
	if err != nil {
		t.Fatalf("Failed to read parquet file: %v", err)
	}

	n := len(records)

	if n != 2 {
		t.Errorf("Expected 2 records, got %d", n)
	}

	// Verify first record
	if records[0].PlanName != "Test Plan 1" {
		t.Errorf("Expected 'Test Plan 1', got '%s'", records[0].PlanName)
	}
	if records[0].URLCount != 2 {
		t.Errorf("Expected URL count 2, got %d", records[0].URLCount)
	}
	if len(records[0].InNetworkURLs) != 2 {
		t.Errorf("Expected 2 URLs, got %d", len(records[0].InNetworkURLs))
	}
	if records[0].InNetworkURLs[0] != "https://example.com/file1.json" {
		t.Errorf("Unexpected first URL: %s", records[0].InNetworkURLs[0])
	}
	if records[0].InNetworkURLs[1] != "https://example.com/file2.json" {
		t.Errorf("Unexpected second URL: %s", records[0].InNetworkURLs[1])
	}

	// Verify second record
	if records[1].PlanName != "Test Plan 2" {
		t.Errorf("Expected 'Test Plan 2', got '%s'", records[1].PlanName)
	}
	if records[1].PlanIDType != "ein" {
		t.Errorf("Expected 'ein', got '%s'", records[1].PlanIDType)
	}
	if records[1].URLCount != 1 {
		t.Errorf("Expected URL count 1, got %d", records[1].URLCount)
	}
	if len(records[1].InNetworkURLs) != 1 {
		t.Errorf("Expected 1 URL, got %d", len(records[1].InNetworkURLs))
	}
	if records[1].InNetworkURLs[0] != "https://example.com/file3.json" {
		t.Errorf("Unexpected URL: %s", records[1].InNetworkURLs[0])
	}
}

// TestUnfilteredJSONParquetParity verifies that unfiltered JSON and Parquet output
// produce identical plan data from the same TOC input.
func TestUnfilteredJSONParquetParity(t *testing.T) {
	// Disable all filters
	CurrentFilter = DefaultFilterConfig()

	tocJSON := `{
		"reporting_entity_name": "Anthem Inc",
		"reporting_entity_type": "health_insurance_issuer",
		"last_updated_on": "2024-06-01",
		"version": "2.0.0",
		"reporting_structure": [
			{
				"reporting_plans": [
					{
						"plan_name": "NY Essential Plan",
						"plan_id_type": "hios",
						"plan_id": "12345NY001",
						"plan_market_type": "individual",
						"issuer_name": "EmblemHealth"
					}
				],
				"in_network_files": [
					{"description": "rates", "location": "https://example.com/ny-rates.json"}
				]
			},
			{
				"reporting_plans": [
					{
						"plan_name": "CA Gold Plan",
						"plan_id_type": "hios",
						"plan_id": "99999CA002",
						"plan_market_type": "individual",
						"issuer_name": "Kaiser Permanente"
					},
					{
						"plan_name": "TX Silver Plan",
						"plan_id_type": "ein",
						"plan_id": "123456789",
						"plan_market_type": "group",
						"issuer_name": "BCBS Texas",
						"plan_sponsor_name": "Acme Corp"
					}
				],
				"in_network_files": [
					{"description": "in-network", "location": "https://example.com/multi1.json"},
					{"description": "behavioral", "location": "https://example.com/multi2.json"}
				]
			},
			{
				"reporting_plans": [
					{
						"plan_name": "Empty URL Plan",
						"plan_id_type": "hios",
						"plan_id": "55555FL003",
						"plan_market_type": "individual",
						"issuer_name": "Florida Blue"
					}
				],
				"in_network_files": []
			}
		]
	}`

	// --- Collect JSON plans ---
	jsonReader := strings.NewReader(tocJSON)
	jsonParser := NewStreamParser(jsonReader)
	var jsonPlans []NYSPlanOutput
	err := jsonParser.Parse(func(plan NYSPlanOutput) {
		jsonPlans = append(jsonPlans, plan)
	}, func(stats ParserStats) {})
	if err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	// --- Write Parquet plans ---
	tmpFile, err := os.CreateTemp("", "parity_test_*.parquet")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	parquetReader := strings.NewReader(tocJSON)
	parquetParser := NewStreamParser(parquetReader)
	pw, err := NewParquetWriter(tmpPath)
	if err != nil {
		t.Fatalf("Failed to create parquet writer: %v", err)
	}
	err = parquetParser.Parse(func(plan NYSPlanOutput) {
		if err := pw.Write(plan); err != nil {
			t.Fatalf("Failed to write parquet: %v", err)
		}
	}, func(stats ParserStats) {})
	if err != nil {
		t.Fatalf("Parquet parse error: %v", err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("Failed to close parquet writer: %v", err)
	}

	// --- Read back Parquet ---
	parquetRecords, err := parquet.ReadFile[NYSPlanParquet](tmpPath)
	if err != nil {
		t.Fatalf("Failed to read parquet file: %v", err)
	}

	// --- Compare counts ---
	if len(jsonPlans) != len(parquetRecords) {
		t.Fatalf("Count mismatch: JSON=%d, Parquet=%d", len(jsonPlans), len(parquetRecords))
	}

	// Expect 4 plans total (1 + 2 + 1) with no filters
	if len(jsonPlans) != 4 {
		t.Fatalf("Expected 4 unfiltered plans, got %d", len(jsonPlans))
	}

	// --- Compare each plan field by field ---
	for i, jp := range jsonPlans {
		pr := parquetRecords[i]

		if jp.PlanName != pr.PlanName {
			t.Errorf("Plan %d PlanName: JSON=%q, Parquet=%q", i, jp.PlanName, pr.PlanName)
		}
		if jp.PlanIDType != pr.PlanIDType {
			t.Errorf("Plan %d PlanIDType: JSON=%q, Parquet=%q", i, jp.PlanIDType, pr.PlanIDType)
		}
		if jp.PlanID != pr.PlanID {
			t.Errorf("Plan %d PlanID: JSON=%q, Parquet=%q", i, jp.PlanID, pr.PlanID)
		}
		if jp.PlanMarketType != pr.PlanMarketType {
			t.Errorf("Plan %d PlanMarketType: JSON=%q, Parquet=%q", i, jp.PlanMarketType, pr.PlanMarketType)
		}
		if jp.IssuerName != pr.IssuerName {
			t.Errorf("Plan %d IssuerName: JSON=%q, Parquet=%q", i, jp.IssuerName, pr.IssuerName)
		}
		if jp.Description != pr.Description {
			t.Errorf("Plan %d Description: JSON=%q, Parquet=%q", i, jp.Description, pr.Description)
		}

		// Compare URL count
		if int32(len(jp.InNetworkURLs)) != pr.URLCount {
			t.Errorf("Plan %d URLCount: JSON len=%d, Parquet=%d", i, len(jp.InNetworkURLs), pr.URLCount)
		}

		// Compare URLs
		if len(jp.InNetworkURLs) != len(pr.InNetworkURLs) {
			t.Errorf("Plan %d URL count: JSON=%d, Parquet=%d", i, len(jp.InNetworkURLs), len(pr.InNetworkURLs))
		} else {
			for j, url := range jp.InNetworkURLs {
				if url != pr.InNetworkURLs[j] {
					t.Errorf("Plan %d URL %d: JSON=%q, Parquet=%q", i, j, url, pr.InNetworkURLs[j])
				}
			}
		}
	}

	// --- Also verify JSON serialization round-trips correctly ---
	// Marshal JSON plans and unmarshal back to confirm no data loss
	jsonBytes, err := json.Marshal(jsonPlans)
	if err != nil {
		t.Fatalf("Failed to marshal JSON plans: %v", err)
	}
	var roundTripped []NYSPlanOutput
	if err := json.Unmarshal(jsonBytes, &roundTripped); err != nil {
		t.Fatalf("Failed to unmarshal JSON plans: %v", err)
	}
	if len(roundTripped) != len(jsonPlans) {
		t.Fatalf("JSON round-trip count mismatch: %d vs %d", len(roundTripped), len(jsonPlans))
	}
	for i, orig := range jsonPlans {
		rt := roundTripped[i]
		if orig.PlanName != rt.PlanName || orig.PlanID != rt.PlanID || orig.IssuerName != rt.IssuerName {
			t.Errorf("Plan %d JSON round-trip mismatch", i)
		}
	}
}
