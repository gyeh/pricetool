package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type npiEntry struct {
	NPI string `json:"npi"`
}

// LoadNPIFilter reads a JSON array of objects with "npi" string fields
// and returns a set of int64 NPIs for filtering.
func LoadNPIFilter(path string) (map[int64]bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read NPI file: %w", err)
	}

	var entries []npiEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse NPI file: %w", err)
	}

	filter := make(map[int64]bool, len(entries))
	for _, e := range entries {
		npi, err := strconv.ParseInt(e.NPI, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid NPI %q: %w", e.NPI, err)
		}
		filter[npi] = true
	}
	return filter, nil
}
