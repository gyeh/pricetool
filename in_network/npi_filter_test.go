package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadNPIFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "npis.json")

	data := `[{"npi":"1234567890","name":"Dr. A"},{"npi":"9876543210","name":"Dr. B"}]`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	filter, err := LoadNPIFilter(path)
	if err != nil {
		t.Fatalf("LoadNPIFilter: %v", err)
	}

	if len(filter) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(filter))
	}
	if !filter[1234567890] {
		t.Error("missing NPI 1234567890")
	}
	if !filter[9876543210] {
		t.Error("missing NPI 9876543210")
	}
}

func TestLoadNPIFilterInvalidNPI(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.json")

	data := `[{"npi":"not-a-number"}]`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadNPIFilter(path)
	if err == nil {
		t.Fatal("expected error for invalid NPI")
	}
}

func TestLoadNPIFilterMissingFile(t *testing.T) {
	_, err := LoadNPIFilter("/nonexistent/path.json")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
