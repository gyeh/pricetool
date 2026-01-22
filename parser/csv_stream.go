package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Column patterns
var (
	csvCodeColPattern     = regexp.MustCompile(`^code\|(\d+)$`)
	csvCodeTypeColPattern = regexp.MustCompile(`^code\|(\d+)\|type$`)
	csvPayerColPattern    = regexp.MustCompile(`^standard_charge\|([^|]+)\|([^|]+)\|(.+)$`)
	csvEstimatedPattern   = regexp.MustCompile(`^estimated_amount\|([^|]+)\|([^|]+)$`)
)

// DetectCSVFormat determines if a CSV is Tall or Wide format based on row 3 headers
func DetectCSVFormat(headers []string) CSVFormat {
	for _, h := range headers {
		if h == "payer_name" || h == "plan_name" {
			return TallCSVFormat
		}
		// Wide format has columns like standard_charge|[payer_name]|[plan_name]|negotiated_dollar
		if strings.Contains(h, "|") && strings.Contains(h, "negotiated_dollar") {
			return WideCSVFormat
		}
	}
	return TallCSVFormat // Default
}

// NewCSVStreamReader creates a streaming CSV reader for large files
func NewCSVStreamReader(filepath string) (*CSVStreamReader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Use buffered reader for better I/O performance
	bufReader := bufio.NewReaderSize(file, 256*1024) // 256KB buffer

	// Skip UTF-8 BOM if present
	bom, err := bufReader.Peek(3)
	if err == nil && len(bom) >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		bufReader.Discard(3)
	}

	reader := csv.NewReader(bufReader)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // Variable fields

	sr := &CSVStreamReader{
		file:   file,
		reader: reader,
		colIdx: make(map[string]int),
	}

	return sr, nil
}

// ReadHeader reads and parses the CSV header (rows 1-3)
func (sr *CSVStreamReader) ReadHeader() (*CSVHeader, error) {
	// Row 1: Header field names
	headerRow, err := sr.reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header row 1: %w", err)
	}
	sr.rowNum++

	// Clean BOM from first column
	if len(headerRow) > 0 {
		headerRow[0] = strings.TrimPrefix(headerRow[0], "\ufeff")
	}

	// Row 2: Header values
	valueRow, err := sr.reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read header row 2: %w", err)
	}
	sr.rowNum++

	// Parse header
	header := parseCSVHeader(headerRow, valueRow)

	// Row 3: Item column headers
	sr.headers, err = sr.reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read item headers row 3: %w", err)
	}
	sr.rowNum++

	// Build column index
	for i, h := range sr.headers {
		h = strings.TrimSpace(h)
		sr.colIdx[h] = i
	}

	// Detect format
	sr.format = DetectCSVFormat(sr.headers)

	// For Wide format, extract payer/plan columns
	if sr.format == WideCSVFormat {
		sr.extractPayerPlans()
	}

	return &header, nil
}

// extractPayerPlans extracts unique payer/plan combinations from Wide format headers
func (sr *CSVStreamReader) extractPayerPlans() {
	seen := make(map[payerPlanKey]bool)

	for _, h := range sr.headers {
		if matches := csvPayerColPattern.FindStringSubmatch(h); matches != nil {
			pp := payerPlanKey{payer: matches[1], plan: matches[2]}
			if !seen[pp] {
				seen[pp] = true
				sr.payerPlans = append(sr.payerPlans, pp)
			}
		}
		if matches := csvEstimatedPattern.FindStringSubmatch(h); matches != nil {
			pp := payerPlanKey{payer: matches[1], plan: matches[2]}
			if !seen[pp] {
				seen[pp] = true
				sr.payerPlans = append(sr.payerPlans, pp)
			}
		}
	}
}

// Format returns the detected CSV format
func (sr *CSVStreamReader) Format() CSVFormat {
	return sr.format
}

// RowNum returns the current row number (1-based)
func (sr *CSVStreamReader) RowNum() int64 {
	return sr.rowNum
}

// Close closes the underlying file
func (sr *CSVStreamReader) Close() error {
	if sr.file != nil {
		return sr.file.Close()
	}
	return nil
}

// NextItem returns the next StandardChargeInformation item
// For Tall format, this groups consecutive rows with the same item key
// For Wide format, each row is one item
// Returns nil, io.EOF when no more items
func (sr *CSVStreamReader) NextItem() (*StandardChargeInformation, error) {
	if sr.format == WideCSVFormat {
		return sr.nextWideItem()
	}
	return sr.nextTallItem()
}

// nextWideItem reads a single row and converts to StandardChargeInformation
func (sr *CSVStreamReader) nextWideItem() (*StandardChargeInformation, error) {
	row, err := sr.reader.Read()
	if err != nil {
		return nil, err
	}
	sr.rowNum++

	// Skip empty rows
	if len(row) == 0 || (len(row) == 1 && row[0] == "") {
		return sr.nextWideItem() // Recursively try next row
	}

	return sr.parseWideRow(row), nil
}

// nextTallItem groups consecutive rows by item key
func (sr *CSVStreamReader) nextTallItem() (*StandardChargeInformation, error) {
	// If we have a pending item from previous call, return it
	if sr.currentItem != nil && sr.pendingRow != nil {
		// Start new item with pending row
		item := sr.currentItem
		sr.currentItem = nil

		// Parse pending row as start of new item
		newKey := sr.getTallItemKey(sr.pendingRow)
		sr.currentItemKey = newKey
		sr.currentItem = sr.parseTallRowBase(sr.pendingRow)
		sr.addTallRowPayer(sr.currentItem, sr.pendingRow)
		sr.pendingRow = nil

		return item, nil
	}

	// Read rows and group by item key
	for {
		row, err := sr.reader.Read()
		if err != nil {
			// EOF - return any buffered item
			if err == io.EOF && sr.currentItem != nil {
				item := sr.currentItem
				sr.currentItem = nil
				return item, nil
			}
			return nil, err
		}
		sr.rowNum++

		// Skip empty rows
		if len(row) == 0 || (len(row) == 1 && row[0] == "") {
			continue
		}

		rowKey := sr.getTallItemKey(row)

		// First row or same item
		if sr.currentItem == nil {
			sr.currentItemKey = rowKey
			sr.currentItem = sr.parseTallRowBase(row)
			sr.addTallRowPayer(sr.currentItem, row)
			continue
		}

		// Same item - add payer info
		if rowKey == sr.currentItemKey {
			sr.addTallRowPayer(sr.currentItem, row)
			continue
		}

		// Different item - save pending row and return current
		sr.pendingRow = row
		item := sr.currentItem
		sr.currentItem = nil
		return item, nil
	}
}

// getTallItemKey generates a key to group rows by item
func (sr *CSVStreamReader) getTallItemKey(row []string) string {
	var parts []string

	// Description
	if idx, ok := sr.colIdx["description"]; ok && idx < len(row) {
		parts = append(parts, row[idx])
	}

	// Codes
	for _, h := range sr.headers {
		if matches := csvCodeColPattern.FindStringSubmatch(h); matches != nil {
			if idx, ok := sr.colIdx[h]; ok && idx < len(row) {
				parts = append(parts, row[idx])
			}
		}
	}

	// Setting
	if idx, ok := sr.colIdx["setting"]; ok && idx < len(row) {
		parts = append(parts, row[idx])
	}

	// Modifiers
	if idx, ok := sr.colIdx["modifiers"]; ok && idx < len(row) {
		parts = append(parts, row[idx])
	}

	return strings.Join(parts, "|")
}

// parseTallRowBase creates the base StandardChargeInformation from a Tall row
func (sr *CSVStreamReader) parseTallRowBase(row []string) *StandardChargeInformation {
	sci := &StandardChargeInformation{}

	// Description
	if idx, ok := sr.colIdx["description"]; ok && idx < len(row) {
		sci.Description = row[idx]
	}

	// Codes
	for _, h := range sr.headers {
		if matches := csvCodeColPattern.FindStringSubmatch(h); matches != nil {
			if idx, ok := sr.colIdx[h]; ok && idx < len(row) && row[idx] != "" {
				codeNum := matches[1]
				typeCol := fmt.Sprintf("code|%s|type", codeNum)
				code := CodeInformation{Code: row[idx]}
				if typeIdx, ok := sr.colIdx[typeCol]; ok && typeIdx < len(row) {
					code.Type = row[typeIdx]
				}
				sci.CodeInformation = append(sci.CodeInformation, code)
			}
		}
	}

	// Drug info
	var drugUnit *float64
	var drugType string
	if idx, ok := sr.colIdx["drug_unit_of_measurement"]; ok && idx < len(row) {
		drugUnit = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["drug_type_of_measurement"]; ok && idx < len(row) {
		drugType = row[idx]
	}
	if drugUnit != nil {
		sci.DrugInformation = &DrugInformation{
			Unit: FlexibleFloat{Value: drugUnit},
			Type: drugType,
		}
	}

	// Standard charge base
	sc := StandardCharge{}

	if idx, ok := sr.colIdx["setting"]; ok && idx < len(row) {
		sc.Setting = row[idx]
	}
	if idx, ok := sr.colIdx["modifiers"]; ok && idx < len(row) && row[idx] != "" {
		sc.ModifierCode = strings.Split(row[idx], "|")
	}
	if idx, ok := sr.colIdx["standard_charge|gross"]; ok && idx < len(row) {
		sc.GrossCharge = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|discounted_cash"]; ok && idx < len(row) {
		sc.DiscountedCash = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|min"]; ok && idx < len(row) {
		sc.Minimum = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|max"]; ok && idx < len(row) {
		sc.Maximum = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["additional_generic_notes"]; ok && idx < len(row) {
		notes := row[idx]
		if notes != "" {
			sc.AdditionalGenericNotes = &notes
		}
	}

	sci.StandardCharges = []StandardCharge{sc}
	return sci
}

// addTallRowPayer adds payer information from a Tall row to an existing item
func (sr *CSVStreamReader) addTallRowPayer(sci *StandardChargeInformation, row []string) {
	if len(sci.StandardCharges) == 0 {
		return
	}

	pi := PayerInformation{}

	if idx, ok := sr.colIdx["payer_name"]; ok && idx < len(row) {
		pi.PayerName = row[idx]
	}
	if idx, ok := sr.colIdx["plan_name"]; ok && idx < len(row) {
		pi.PlanName = row[idx]
	}
	if idx, ok := sr.colIdx["standard_charge|negotiated_dollar"]; ok && idx < len(row) {
		pi.StandardChargeDollar = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|negotiated_percentage"]; ok && idx < len(row) {
		pi.StandardChargePercentage = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|negotiated_algorithm"]; ok && idx < len(row) {
		algo := row[idx]
		if algo != "" {
			pi.StandardChargeAlgorithm = &algo
		}
	}
	if idx, ok := sr.colIdx["estimated_amount"]; ok && idx < len(row) {
		pi.EstimatedAmount = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|methodology"]; ok && idx < len(row) {
		pi.Methodology = row[idx]
	}

	// Only add if there's meaningful payer data
	if pi.PayerName != "" {
		sci.StandardCharges[0].PayersInformation = append(
			sci.StandardCharges[0].PayersInformation, pi)
	}
}

// parseWideRow parses a Wide format row into StandardChargeInformation
func (sr *CSVStreamReader) parseWideRow(row []string) *StandardChargeInformation {
	sci := &StandardChargeInformation{}

	// Description
	if idx, ok := sr.colIdx["description"]; ok && idx < len(row) {
		sci.Description = row[idx]
	}

	// Codes
	for _, h := range sr.headers {
		if matches := csvCodeColPattern.FindStringSubmatch(h); matches != nil {
			if idx, ok := sr.colIdx[h]; ok && idx < len(row) && row[idx] != "" {
				codeNum := matches[1]
				typeCol := fmt.Sprintf("code|%s|type", codeNum)
				code := CodeInformation{Code: row[idx]}
				if typeIdx, ok := sr.colIdx[typeCol]; ok && typeIdx < len(row) {
					code.Type = row[typeIdx]
				}
				sci.CodeInformation = append(sci.CodeInformation, code)
			}
		}
	}

	// Drug info
	var drugUnit *float64
	var drugType string
	if idx, ok := sr.colIdx["drug_unit_of_measurement"]; ok && idx < len(row) {
		drugUnit = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["drug_type_of_measurement"]; ok && idx < len(row) {
		drugType = row[idx]
	}
	if drugUnit != nil {
		sci.DrugInformation = &DrugInformation{
			Unit: FlexibleFloat{Value: drugUnit},
			Type: drugType,
		}
	}

	// Standard charge
	sc := StandardCharge{}

	if idx, ok := sr.colIdx["setting"]; ok && idx < len(row) {
		sc.Setting = row[idx]
	}
	if idx, ok := sr.colIdx["modifiers"]; ok && idx < len(row) && row[idx] != "" {
		sc.ModifierCode = strings.Split(row[idx], "|")
	}
	if idx, ok := sr.colIdx["standard_charge|gross"]; ok && idx < len(row) {
		sc.GrossCharge = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|discounted_cash"]; ok && idx < len(row) {
		sc.DiscountedCash = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|min"]; ok && idx < len(row) {
		sc.Minimum = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["standard_charge|max"]; ok && idx < len(row) {
		sc.Maximum = parseFloat(row[idx])
	}
	if idx, ok := sr.colIdx["additional_generic_notes"]; ok && idx < len(row) {
		notes := row[idx]
		if notes != "" {
			sc.AdditionalGenericNotes = &notes
		}
	}

	// Parse payer-specific charges
	for _, pp := range sr.payerPlans {
		pi := PayerInformation{
			PayerName: strings.ReplaceAll(pp.payer, "_", " "),
			PlanName:  pp.plan,
		}

		dollarCol := fmt.Sprintf("standard_charge|%s|%s|negotiated_dollar", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[dollarCol]; ok && idx < len(row) {
			pi.StandardChargeDollar = parseFloat(row[idx])
		}

		pctCol := fmt.Sprintf("standard_charge|%s|%s|negotiated_percentage", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[pctCol]; ok && idx < len(row) {
			pi.StandardChargePercentage = parseFloat(row[idx])
		}

		algoCol := fmt.Sprintf("standard_charge|%s|%s|negotiated_algorithm", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[algoCol]; ok && idx < len(row) {
			algo := row[idx]
			if algo != "" {
				pi.StandardChargeAlgorithm = &algo
			}
		}

		estCol := fmt.Sprintf("estimated_amount|%s|%s", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[estCol]; ok && idx < len(row) {
			pi.EstimatedAmount = parseFloat(row[idx])
		}

		methodCol := fmt.Sprintf("standard_charge|%s|%s|methodology", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[methodCol]; ok && idx < len(row) {
			pi.Methodology = row[idx]
		}

		notesCol := fmt.Sprintf("additional_payer_notes|%s|%s", pp.payer, pp.plan)
		if idx, ok := sr.colIdx[notesCol]; ok && idx < len(row) {
			notes := row[idx]
			if notes != "" {
				pi.AdditionalPayerNotes = &notes
			}
		}

		// Only add if there's some data for this payer
		if pi.StandardChargeDollar != nil || pi.StandardChargePercentage != nil ||
			pi.StandardChargeAlgorithm != nil || pi.EstimatedAmount != nil ||
			pi.Methodology != "" || pi.AdditionalPayerNotes != nil {
			sc.PayersInformation = append(sc.PayersInformation, pi)
		}
	}

	sci.StandardCharges = []StandardCharge{sc}
	return sci
}

// parseCSVHeader parses the hospital header from rows 1-2
func parseCSVHeader(headerRow, valueRow []string) CSVHeader {
	header := CSVHeader{
		LicenseNumbers: make(map[string]string),
	}

	// Clean BOM from first column if present
	if len(headerRow) > 0 {
		headerRow[0] = strings.TrimPrefix(headerRow[0], "\ufeff")
	}

	for i, col := range headerRow {
		if i >= len(valueRow) {
			break
		}
		value := valueRow[i]

		switch {
		case col == "hospital_name":
			header.HospitalName = value
		case col == "last_updated_on":
			header.LastUpdatedOn = value
		case col == "version":
			header.Version = value
		case col == "hospital_location":
			if value != "" {
				header.HospitalLocations = strings.Split(value, "|")
			}
		case col == "hospital_address":
			if value != "" {
				header.HospitalAddresses = strings.Split(value, "|")
			}
		case strings.HasPrefix(col, "license_number|"):
			state := strings.TrimPrefix(col, "license_number|")
			header.LicenseNumbers[state] = value
		case strings.Contains(col, "knowledge and belief"):
			header.Affirmation = col
			header.ConfirmAffirmation = strings.ToUpper(value) == "TRUE"
		case col == "general_contract_provisions":
			header.GeneralContractProvisions = value
		}
	}

	return header
}

// parseFloat parses a string to float64 pointer, returns nil for empty strings
func parseFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	// Remove commas
	s = strings.ReplaceAll(s, ",", "")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}
