package main

import (
	"encoding/csv"
	"os"
)

type CSVFormat string

type CSVHeader struct {
	HospitalName              string
	LastUpdatedOn             string
	Version                   string
	HospitalLocations         []string
	HospitalAddresses         []string
	LicenseNumbers            map[string]string // state -> license number
	Affirmation               string
	ConfirmAffirmation        bool
	GeneralContractProvisions string
}

// CSVStreamReader provides streaming access to large CSV files
type CSVStreamReader struct {
	file       *os.File
	reader     *csv.Reader
	headers    []string
	format     CSVFormat
	colIdx     map[string]int
	payerPlans []payerPlanKey // For Wide format
	rowNum     int64

	// For Tall format grouping - buffer to accumulate rows for same item
	currentItemKey string
	currentItem    *StandardChargeInformation
	pendingRow     []string // Row that belongs to next item
}

type payerPlanKey struct {
	payer string
	plan  string
}

const (
	TallCSVFormat CSVFormat = "tall"
	WideCSVFormat CSVFormat = "wide"
)
