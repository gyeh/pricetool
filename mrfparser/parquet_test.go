package main

import (
	"os"
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
