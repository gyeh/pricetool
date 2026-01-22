package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestParseHospitalHeader(t *testing.T) {
	jsonData := `{
		"hospital_name": "Test Hospital",
		"last_updated_on": "2025-01-15",
		"version": "3.0.0",
		"location_name": ["Main Campus", "Surgical Center"],
		"hospital_address": ["123 Main St, City, ST 12345"],
		"license_information": {
			"license_number": "12345",
			"state": "CA"
		},
		"type_2_npi": ["1234567890"],
		"attestation": {
			"attestation": "Test attestation text",
			"confirm_attestation": true,
			"attester_name": "John Doe"
		},
		"standard_charge_information": [],
		"modifier_information": []
	}`

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonData)))

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		t.Fatalf("Expected opening brace, got %v", token)
	}

	header := &HospitalHeader{}

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatalf("Failed to read field name: %v", err)
		}

		fieldName, ok := token.(string)
		if !ok {
			t.Fatalf("Expected field name string, got %v", token)
		}

		switch fieldName {
		case "hospital_name":
			if err := decoder.Decode(&header.HospitalName); err != nil {
				t.Fatalf("Failed to decode hospital_name: %v", err)
			}
		case "hospital_address":
			if err := decoder.Decode(&header.HospitalAddress); err != nil {
				t.Fatalf("Failed to decode hospital_address: %v", err)
			}
		case "last_updated_on":
			if err := decoder.Decode(&header.LastUpdatedOn); err != nil {
				t.Fatalf("Failed to decode last_updated_on: %v", err)
			}
		case "attestation":
			if err := decoder.Decode(&header.Attestation); err != nil {
				t.Fatalf("Failed to decode attestation: %v", err)
			}
		case "license_information":
			if err := decoder.Decode(&header.LicenseInformation); err != nil {
				t.Fatalf("Failed to decode license_information: %v", err)
			}
		case "version":
			if err := decoder.Decode(&header.Version); err != nil {
				t.Fatalf("Failed to decode version: %v", err)
			}
		case "location_name":
			if err := decoder.Decode(&header.LocationName); err != nil {
				t.Fatalf("Failed to decode location_name: %v", err)
			}
		case "type_2_npi":
			if err := decoder.Decode(&header.Type2NPI); err != nil {
				t.Fatalf("Failed to decode type_2_npi: %v", err)
			}
		default:
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				t.Fatalf("Failed to skip field %s: %v", fieldName, err)
			}
		}
	}

	// Verify parsed values
	if header.HospitalName != "Test Hospital" {
		t.Errorf("Expected hospital_name 'Test Hospital', got '%s'", header.HospitalName)
	}
	if header.LastUpdatedOn != "2025-01-15" {
		t.Errorf("Expected last_updated_on '2025-01-15', got '%s'", header.LastUpdatedOn)
	}
	if header.Version != "3.0.0" {
		t.Errorf("Expected version '3.0.0', got '%s'", header.Version)
	}
	if len(header.LocationName) != 2 {
		t.Errorf("Expected 2 location names, got %d", len(header.LocationName))
	}
	if header.LicenseInformation.State != "CA" {
		t.Errorf("Expected license state 'CA', got '%s'", header.LicenseInformation.State)
	}
	if header.Attestation.AttesterName != "John Doe" {
		t.Errorf("Expected attester name 'John Doe', got '%s'", header.Attestation.AttesterName)
	}
}

func TestParseStandardChargeInformation(t *testing.T) {
	jsonData := `{
		"description": "Major hip and knee joint replacement",
		"code_information": [
			{"code": "470", "type": "MS-DRG"},
			{"code": "175869", "type": "LOCAL"}
		],
		"standard_charges": [
			{
				"setting": "inpatient",
				"minimum": 25678,
				"maximum": 25678,
				"gross_charge": 50000,
				"discounted_cash": 40000,
				"payers_information": [
					{
						"payer_name": "Test Insurance",
						"plan_name": "PPO",
						"standard_charge_dollar": 25678,
						"methodology": "case rate"
					}
				]
			}
		]
	}`

	var sci StandardChargeInformation
	if err := json.Unmarshal([]byte(jsonData), &sci); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if sci.Description != "Major hip and knee joint replacement" {
		t.Errorf("Expected description 'Major hip and knee joint replacement', got '%s'", sci.Description)
	}
	if len(sci.CodeInformation) != 2 {
		t.Errorf("Expected 2 code information entries, got %d", len(sci.CodeInformation))
	}
	if sci.CodeInformation[0].Code != "470" {
		t.Errorf("Expected code '470', got '%s'", sci.CodeInformation[0].Code)
	}
	if sci.CodeInformation[0].Type != "MS-DRG" {
		t.Errorf("Expected type 'MS-DRG', got '%s'", sci.CodeInformation[0].Type)
	}
	if len(sci.StandardCharges) != 1 {
		t.Errorf("Expected 1 standard charge, got %d", len(sci.StandardCharges))
	}
	if sci.StandardCharges[0].Setting != "inpatient" {
		t.Errorf("Expected setting 'inpatient', got '%s'", sci.StandardCharges[0].Setting)
	}
	if sci.StandardCharges[0].GrossCharge == nil || *sci.StandardCharges[0].GrossCharge != 50000 {
		t.Errorf("Expected gross_charge 50000, got %v", sci.StandardCharges[0].GrossCharge)
	}
	if len(sci.StandardCharges[0].PayersInformation) != 1 {
		t.Errorf("Expected 1 payer information, got %d", len(sci.StandardCharges[0].PayersInformation))
	}
	if sci.StandardCharges[0].PayersInformation[0].Methodology != "case rate" {
		t.Errorf("Expected methodology 'case rate', got '%s'", sci.StandardCharges[0].PayersInformation[0].Methodology)
	}
}

func TestParseStandardChargeWithDrugInfo(t *testing.T) {
	jsonData := `{
		"description": "Aspirin 81 milligram chewable tablet",
		"drug_information": {
			"unit": 1,
			"type": "UN"
		},
		"code_information": [
			{"code": "10135-0729-62", "type": "NDC"}
		],
		"standard_charges": [
			{
				"setting": "both",
				"gross_charge": 2,
				"discounted_cash": 1.5,
				"minimum": 0.75,
				"maximum": 0.95,
				"payers_information": [
					{
						"payer_name": "Test Insurance",
						"plan_name": "PPO",
						"standard_charge_dollar": 0.75,
						"methodology": "fee schedule"
					}
				]
			}
		]
	}`

	var sci StandardChargeInformation
	if err := json.Unmarshal([]byte(jsonData), &sci); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if sci.DrugInformation == nil {
		t.Fatal("Expected drug_information to be present")
	}
	if sci.DrugInformation.Unit.Value == nil || *sci.DrugInformation.Unit.Value != 1 {
		t.Errorf("Expected drug unit 1, got %v", sci.DrugInformation.Unit.Value)
	}
	if sci.DrugInformation.Type != "UN" {
		t.Errorf("Expected drug type 'UN', got '%s'", sci.DrugInformation.Type)
	}
	if sci.CodeInformation[0].Type != "NDC" {
		t.Errorf("Expected code type 'NDC', got '%s'", sci.CodeInformation[0].Type)
	}
}

func TestParsePayerInformationWithPercentage(t *testing.T) {
	jsonData := `{
		"payer_name": "Platform Health Insurance",
		"plan_name": "PPO",
		"standard_charge_percentage": 50,
		"median_amount": 21345.12,
		"10th_percentile": 18765.9,
		"90th_percentile": 39627.88,
		"count": "23",
		"methodology": "percent of total billed charges"
	}`

	var pi PayerInformation
	if err := json.Unmarshal([]byte(jsonData), &pi); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if pi.PayerName != "Platform Health Insurance" {
		t.Errorf("Expected payer_name 'Platform Health Insurance', got '%s'", pi.PayerName)
	}
	if pi.StandardChargePercentage == nil || *pi.StandardChargePercentage != 50 {
		t.Errorf("Expected standard_charge_percentage 50, got %v", pi.StandardChargePercentage)
	}
	if pi.MedianAmount == nil || *pi.MedianAmount != 21345.12 {
		t.Errorf("Expected median_amount 21345.12, got %v", pi.MedianAmount)
	}
	if pi.Percentile10th == nil || *pi.Percentile10th != 18765.9 {
		t.Errorf("Expected 10th_percentile 18765.9, got %v", pi.Percentile10th)
	}
	if pi.Percentile90th == nil || *pi.Percentile90th != 39627.88 {
		t.Errorf("Expected 90th_percentile 39627.88, got %v", pi.Percentile90th)
	}
	if pi.Count == nil || *pi.Count != "23" {
		t.Errorf("Expected count '23', got %v", pi.Count)
	}
}

func TestParsePayerInformationWithAlgorithm(t *testing.T) {
	jsonData := `{
		"payer_name": "Region Health Insurance",
		"plan_name": "HMO",
		"standard_charge_dollar": 25678,
		"standard_charge_algorithm": "If days in visit is less than or equal to 3, then the standard charge is $25,678.00.",
		"median_amount": 25678,
		"10th_percentile": 25678,
		"90th_percentile": 45964,
		"count": "43",
		"methodology": "other",
		"additional_payer_notes": "The standard charge methodology is a case rate plus additional modifications."
	}`

	var pi PayerInformation
	if err := json.Unmarshal([]byte(jsonData), &pi); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if pi.StandardChargeDollar == nil || *pi.StandardChargeDollar != 25678 {
		t.Errorf("Expected standard_charge_dollar 25678, got %v", pi.StandardChargeDollar)
	}
	if pi.StandardChargeAlgorithm == nil {
		t.Error("Expected standard_charge_algorithm to be present")
	}
	if pi.Methodology != "other" {
		t.Errorf("Expected methodology 'other', got '%s'", pi.Methodology)
	}
	if pi.AdditionalPayerNotes == nil {
		t.Error("Expected additional_payer_notes to be present")
	}
}

func TestParseModifierInformation(t *testing.T) {
	jsonData := `{
		"description": "Bilateral procedure",
		"code": "50",
		"setting": "both",
		"modifier_payer_information": [
			{
				"payer_name": "Platform Health Insurance",
				"plan_name": "PPO",
				"description": "150% payment adjustment for the item or service"
			},
			{
				"payer_name": "Region Health Insurance",
				"plan_name": "HMO",
				"description": "145% payment adjustment for the item or service"
			}
		]
	}`

	var mod ModifierInformation
	if err := json.Unmarshal([]byte(jsonData), &mod); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if mod.Description != "Bilateral procedure" {
		t.Errorf("Expected description 'Bilateral procedure', got '%s'", mod.Description)
	}
	if mod.Code != "50" {
		t.Errorf("Expected code '50', got '%s'", mod.Code)
	}
	if mod.Setting == nil || *mod.Setting != "both" {
		t.Errorf("Expected setting 'both', got %v", mod.Setting)
	}
	if len(mod.ModifierPayerInformation) != 2 {
		t.Errorf("Expected 2 modifier payer info entries, got %d", len(mod.ModifierPayerInformation))
	}
}

func TestParseSampleFile(t *testing.T) {
	// Find the sample file in testdata directory (parent directory)
	sampleFile := filepath.Join("..", "testdata", "v3_json_format_example.json")
	if _, err := os.Stat(sampleFile); os.IsNotExist(err) {
		t.Fatalf("Sample file not found at %s", sampleFile)
	}

	file, err := os.Open(sampleFile)
	if err != nil {
		t.Fatalf("Failed to open sample file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		t.Fatalf("Expected opening brace, got %v", token)
	}

	header := &HospitalHeader{}
	var standardCharges []StandardChargeInformation
	var modifiers []ModifierInformation

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatalf("Failed to read field name: %v", err)
		}

		fieldName, ok := token.(string)
		if !ok {
			t.Fatalf("Expected field name string, got %v", token)
		}

		switch fieldName {
		case "hospital_name":
			if err := decoder.Decode(&header.HospitalName); err != nil {
				t.Fatalf("Failed to decode hospital_name: %v", err)
			}
		case "hospital_address":
			if err := decoder.Decode(&header.HospitalAddress); err != nil {
				t.Fatalf("Failed to decode hospital_address: %v", err)
			}
		case "last_updated_on":
			if err := decoder.Decode(&header.LastUpdatedOn); err != nil {
				t.Fatalf("Failed to decode last_updated_on: %v", err)
			}
		case "attestation":
			if err := decoder.Decode(&header.Attestation); err != nil {
				t.Fatalf("Failed to decode attestation: %v", err)
			}
		case "license_information":
			if err := decoder.Decode(&header.LicenseInformation); err != nil {
				t.Fatalf("Failed to decode license_information: %v", err)
			}
		case "version":
			if err := decoder.Decode(&header.Version); err != nil {
				t.Fatalf("Failed to decode version: %v", err)
			}
		case "location_name":
			if err := decoder.Decode(&header.LocationName); err != nil {
				t.Fatalf("Failed to decode location_name: %v", err)
			}
		case "type_2_npi":
			if err := decoder.Decode(&header.Type2NPI); err != nil {
				t.Fatalf("Failed to decode type_2_npi: %v", err)
			}
		case "standard_charge_information":
			if err := decoder.Decode(&standardCharges); err != nil {
				t.Fatalf("Failed to decode standard_charge_information: %v", err)
			}
		case "modifier_information":
			if err := decoder.Decode(&modifiers); err != nil {
				t.Fatalf("Failed to decode modifier_information: %v", err)
			}
		default:
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				t.Fatalf("Failed to skip field %s: %v", fieldName, err)
			}
		}
	}

	// Verify expected values from the sample file
	if header.HospitalName != "West Mercy Hospital" {
		t.Errorf("Expected hospital_name 'West Mercy Hospital', got '%s'", header.HospitalName)
	}
	if header.Version != "3.0.0" {
		t.Errorf("Expected version '3.0.0', got '%s'", header.Version)
	}
	if len(header.LocationName) != 2 {
		t.Errorf("Expected 2 location names, got %d", len(header.LocationName))
	}
	if len(header.HospitalAddress) != 2 {
		t.Errorf("Expected 2 hospital addresses, got %d", len(header.HospitalAddress))
	}
	if header.LicenseInformation.State != "CA" {
		t.Errorf("Expected license state 'CA', got '%s'", header.LicenseInformation.State)
	}
	if len(header.Type2NPI) != 3 {
		t.Errorf("Expected 3 NPIs, got %d", len(header.Type2NPI))
	}

	// Verify standard charges
	if len(standardCharges) != 12 {
		t.Errorf("Expected 12 standard charge items, got %d", len(standardCharges))
	}

	// Verify first standard charge item
	if standardCharges[0].Description != "Major hip and knee joint replacement or reattachment of lower extremity without mcc" {
		t.Errorf("Unexpected first item description: %s", standardCharges[0].Description)
	}
	if len(standardCharges[0].CodeInformation) != 2 {
		t.Errorf("Expected 2 code information entries for first item, got %d", len(standardCharges[0].CodeInformation))
	}

	// Verify drug information is present for NDC items
	foundDrugItem := false
	for _, sci := range standardCharges {
		for _, code := range sci.CodeInformation {
			if code.Type == "NDC" {
				if sci.DrugInformation == nil {
					t.Errorf("Expected drug_information for NDC code item: %s", sci.Description)
				} else {
					foundDrugItem = true
				}
				break
			}
		}
	}
	if !foundDrugItem {
		t.Error("Expected to find at least one item with NDC code and drug information")
	}

	// Verify modifiers
	if len(modifiers) != 3 {
		t.Errorf("Expected 3 modifiers, got %d", len(modifiers))
	}
	if modifiers[0].Code != "50" {
		t.Errorf("Expected first modifier code '50', got '%s'", modifiers[0].Code)
	}
}

func TestStreamingParseStandardCharges(t *testing.T) {
	// Test the streaming array parsing logic
	jsonData := `[
		{
			"description": "Item 1",
			"code_information": [{"code": "001", "type": "CPT"}],
			"standard_charges": [{"setting": "inpatient", "gross_charge": 100}]
		},
		{
			"description": "Item 2",
			"code_information": [{"code": "002", "type": "HCPCS"}],
			"standard_charges": [{"setting": "outpatient", "gross_charge": 200}]
		},
		{
			"description": "Item 3",
			"code_information": [{"code": "003", "type": "MS-DRG"}],
			"standard_charges": [{"setting": "both", "gross_charge": 300}]
		}
	]`

	decoder := json.NewDecoder(bytes.NewReader([]byte(jsonData)))

	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read array start: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		t.Fatalf("Expected array start, got %v", token)
	}

	var items []StandardChargeInformation
	count := 0

	// Stream through elements
	for decoder.More() {
		var sci StandardChargeInformation
		if err := decoder.Decode(&sci); err != nil {
			t.Fatalf("Failed to decode item %d: %v", count, err)
		}
		items = append(items, sci)
		count++
	}

	// Read closing bracket
	token, err = decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read array end: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected 3 items, got %d", count)
	}
	if items[0].Description != "Item 1" {
		t.Errorf("Expected 'Item 1', got '%s'", items[0].Description)
	}
	if items[1].CodeInformation[0].Type != "HCPCS" {
		t.Errorf("Expected 'HCPCS', got '%s'", items[1].CodeInformation[0].Type)
	}
	if items[2].StandardCharges[0].Setting != "both" {
		t.Errorf("Expected 'both', got '%s'", items[2].StandardCharges[0].Setting)
	}
}

func TestParseOptionalFields(t *testing.T) {
	// Test that optional fields are correctly handled as nil
	jsonData := `{
		"description": "Simple item",
		"code_information": [{"code": "001", "type": "CPT"}],
		"standard_charges": [
			{
				"setting": "outpatient",
				"payers_information": [
					{
						"payer_name": "Test Payer",
						"plan_name": "Basic",
						"standard_charge_dollar": 100,
						"methodology": "fee schedule"
					}
				]
			}
		]
	}`

	var sci StandardChargeInformation
	if err := json.Unmarshal([]byte(jsonData), &sci); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	// Verify optional fields are nil
	if sci.DrugInformation != nil {
		t.Error("Expected drug_information to be nil")
	}
	if sci.StandardCharges[0].GrossCharge != nil {
		t.Error("Expected gross_charge to be nil")
	}
	if sci.StandardCharges[0].DiscountedCash != nil {
		t.Error("Expected discounted_cash to be nil")
	}
	if sci.StandardCharges[0].Minimum != nil {
		t.Error("Expected minimum to be nil")
	}
	if sci.StandardCharges[0].Maximum != nil {
		t.Error("Expected maximum to be nil")
	}
	if sci.StandardCharges[0].AdditionalGenericNotes != nil {
		t.Error("Expected additional_generic_notes to be nil")
	}

	pi := sci.StandardCharges[0].PayersInformation[0]
	if pi.StandardChargePercentage != nil {
		t.Error("Expected standard_charge_percentage to be nil")
	}
	if pi.StandardChargeAlgorithm != nil {
		t.Error("Expected standard_charge_algorithm to be nil")
	}
	if pi.MedianAmount != nil {
		t.Error("Expected median_amount to be nil")
	}
	if pi.AdditionalPayerNotes != nil {
		t.Error("Expected additional_payer_notes to be nil")
	}
}

func TestParseAllCodeTypes(t *testing.T) {
	codeTypes := []string{
		"CPT", "HCPCS", "ICD", "DRG", "MS-DRG", "R-DRG", "S-DRG",
		"APS-DRG", "AP-DRG", "APR-DRG", "TRIS-DRG", "APC", "NDC",
		"HIPPS", "LOCAL", "EAPG", "CDT", "RC", "CDM", "CMG", "MS-LTC-DRG",
	}

	for _, codeType := range codeTypes {
		jsonData := `{"code": "123", "type": "` + codeType + `"}`
		var ci CodeInformation
		if err := json.Unmarshal([]byte(jsonData), &ci); err != nil {
			t.Errorf("Failed to unmarshal code type %s: %v", codeType, err)
		}
		if ci.Type != codeType {
			t.Errorf("Expected code type '%s', got '%s'", codeType, ci.Type)
		}
	}
}

func TestParseAllMethodologies(t *testing.T) {
	methodologies := []string{
		"case rate",
		"fee schedule",
		"percent of total billed charges",
		"per diem",
		"other",
	}

	for _, methodology := range methodologies {
		jsonData := `{
			"payer_name": "Test",
			"plan_name": "Test",
			"standard_charge_dollar": 100,
			"methodology": "` + methodology + `"
		}`
		var pi PayerInformation
		if err := json.Unmarshal([]byte(jsonData), &pi); err != nil {
			t.Errorf("Failed to unmarshal methodology %s: %v", methodology, err)
		}
		if pi.Methodology != methodology {
			t.Errorf("Expected methodology '%s', got '%s'", methodology, pi.Methodology)
		}
	}
}

func TestParseAllSettings(t *testing.T) {
	settings := []string{"inpatient", "outpatient", "both"}

	for _, setting := range settings {
		jsonData := `{"setting": "` + setting + `", "gross_charge": 100}`
		var sc StandardCharge
		if err := json.Unmarshal([]byte(jsonData), &sc); err != nil {
			t.Errorf("Failed to unmarshal setting %s: %v", setting, err)
		}
		if sc.Setting != setting {
			t.Errorf("Expected setting '%s', got '%s'", setting, sc.Setting)
		}
	}
}

func TestParseAllDrugUnitTypes(t *testing.T) {
	drugTypes := []string{"GR", "ML", "ME", "UN", "F2", "GM", "EA"}

	for _, drugType := range drugTypes {
		jsonData := `{"unit": 1, "type": "` + drugType + `"}`
		var di DrugInformation
		if err := json.Unmarshal([]byte(jsonData), &di); err != nil {
			t.Errorf("Failed to unmarshal drug type %s: %v", drugType, err)
		}
		if di.Type != drugType {
			t.Errorf("Expected drug type '%s', got '%s'", drugType, di.Type)
		}
	}
}

// TestParseV2File tests parsing the V2 format example file
// V2 uses: hospital_location, affirmation, drug unit as string
func TestParseV2File(t *testing.T) {
	sampleFile := filepath.Join("..", "testdata", "v2_json_format_example.json")
	if _, err := os.Stat(sampleFile); os.IsNotExist(err) {
		t.Fatalf("V2 sample file not found at %s", sampleFile)
	}

	file, err := os.Open(sampleFile)
	if err != nil {
		t.Fatalf("Failed to open V2 sample file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		t.Fatalf("Expected opening brace, got %v", token)
	}

	header := &HospitalHeader{}
	var standardCharges []StandardChargeInformation
	var modifiers []ModifierInformation

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatalf("Failed to read field name: %v", err)
		}

		fieldName, ok := token.(string)
		if !ok {
			t.Fatalf("Expected field name string, got %v", token)
		}

		switch fieldName {
		case "hospital_name":
			if err := decoder.Decode(&header.HospitalName); err != nil {
				t.Fatalf("Failed to decode hospital_name: %v", err)
			}
		case "hospital_address":
			if err := decoder.Decode(&header.HospitalAddress); err != nil {
				t.Fatalf("Failed to decode hospital_address: %v", err)
			}
		case "last_updated_on":
			if err := decoder.Decode(&header.LastUpdatedOn); err != nil {
				t.Fatalf("Failed to decode last_updated_on: %v", err)
			}
		case "affirmation":
			// V2 uses "affirmation" instead of "attestation"
			if err := decoder.Decode(&header.Affirmation); err != nil {
				t.Fatalf("Failed to decode affirmation: %v", err)
			}
		case "license_information":
			if err := decoder.Decode(&header.LicenseInformation); err != nil {
				t.Fatalf("Failed to decode license_information: %v", err)
			}
		case "version":
			if err := decoder.Decode(&header.Version); err != nil {
				t.Fatalf("Failed to decode version: %v", err)
			}
		case "hospital_location":
			// V2 uses "hospital_location" instead of "location_name"
			if err := decoder.Decode(&header.HospitalLocation); err != nil {
				t.Fatalf("Failed to decode hospital_location: %v", err)
			}
		case "standard_charge_information":
			if err := decoder.Decode(&standardCharges); err != nil {
				t.Fatalf("Failed to decode standard_charge_information: %v", err)
			}
		case "modifier_information":
			if err := decoder.Decode(&modifiers); err != nil {
				t.Fatalf("Failed to decode modifier_information: %v", err)
			}
		default:
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				t.Fatalf("Failed to skip field %s: %v", fieldName, err)
			}
		}
	}

	// Verify V2-specific header values
	if header.HospitalName != "West Mercy Hospital" {
		t.Errorf("Expected hospital_name 'West Mercy Hospital', got '%s'", header.HospitalName)
	}
	if header.Version != "2.0.0" {
		t.Errorf("Expected version '2.0.0', got '%s'", header.Version)
	}

	// V2 uses hospital_location instead of location_name
	if len(header.HospitalLocation) != 2 {
		t.Errorf("Expected 2 hospital_location entries, got %d", len(header.HospitalLocation))
	}
	if len(header.HospitalLocation) > 0 && header.HospitalLocation[0] != "West Mercy Hospital" {
		t.Errorf("Expected first hospital_location 'West Mercy Hospital', got '%s'", header.HospitalLocation[0])
	}

	// V2 uses affirmation instead of attestation
	expectedAffirmation := "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated."
	if header.Affirmation.Affirmation != expectedAffirmation {
		t.Errorf("Expected V2 affirmation text, got '%s'", header.Affirmation.Affirmation)
	}
	if !header.Affirmation.ConfirmAffirmation {
		t.Error("Expected confirm_affirmation to be true")
	}

	// Verify standard charges
	if len(standardCharges) != 11 {
		t.Errorf("Expected 11 standard charge items in V2 file, got %d", len(standardCharges))
	}

	// Find a drug item and verify V2's string drug unit
	for _, sci := range standardCharges {
		if sci.DrugInformation != nil {
			// V2 schema has drug unit as string, but FlexibleFloat handles both
			if sci.DrugInformation.Unit.Value == nil {
				t.Error("Expected drug unit to be parsed")
			}
			break
		}
	}

	// Verify modifiers
	if len(modifiers) != 3 {
		t.Errorf("Expected 3 modifiers in V2 file, got %d", len(modifiers))
	}
}

// TestParseV3File tests parsing the V3 format example file
// V3 uses: location_name, attestation with attester_name, type_2_npi, drug unit as number
func TestParseV3File(t *testing.T) {
	sampleFile := filepath.Join("..", "testdata", "v3_json_format_example.json")
	if _, err := os.Stat(sampleFile); os.IsNotExist(err) {
		t.Fatalf("V3 sample file not found at %s", sampleFile)
	}

	file, err := os.Open(sampleFile)
	if err != nil {
		t.Fatalf("Failed to open V3 sample file: %v", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("Failed to read opening token: %v", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		t.Fatalf("Expected opening brace, got %v", token)
	}

	header := &HospitalHeader{}
	var standardCharges []StandardChargeInformation
	var modifiers []ModifierInformation

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			t.Fatalf("Failed to read field name: %v", err)
		}

		fieldName, ok := token.(string)
		if !ok {
			t.Fatalf("Expected field name string, got %v", token)
		}

		switch fieldName {
		case "hospital_name":
			if err := decoder.Decode(&header.HospitalName); err != nil {
				t.Fatalf("Failed to decode hospital_name: %v", err)
			}
		case "hospital_address":
			if err := decoder.Decode(&header.HospitalAddress); err != nil {
				t.Fatalf("Failed to decode hospital_address: %v", err)
			}
		case "last_updated_on":
			if err := decoder.Decode(&header.LastUpdatedOn); err != nil {
				t.Fatalf("Failed to decode last_updated_on: %v", err)
			}
		case "attestation":
			// V3 uses "attestation" with attester_name
			if err := decoder.Decode(&header.Attestation); err != nil {
				t.Fatalf("Failed to decode attestation: %v", err)
			}
		case "license_information":
			if err := decoder.Decode(&header.LicenseInformation); err != nil {
				t.Fatalf("Failed to decode license_information: %v", err)
			}
		case "version":
			if err := decoder.Decode(&header.Version); err != nil {
				t.Fatalf("Failed to decode version: %v", err)
			}
		case "location_name":
			// V3 uses "location_name" instead of "hospital_location"
			if err := decoder.Decode(&header.LocationName); err != nil {
				t.Fatalf("Failed to decode location_name: %v", err)
			}
		case "type_2_npi":
			// V3 has type_2_npi field
			if err := decoder.Decode(&header.Type2NPI); err != nil {
				t.Fatalf("Failed to decode type_2_npi: %v", err)
			}
		case "standard_charge_information":
			if err := decoder.Decode(&standardCharges); err != nil {
				t.Fatalf("Failed to decode standard_charge_information: %v", err)
			}
		case "modifier_information":
			if err := decoder.Decode(&modifiers); err != nil {
				t.Fatalf("Failed to decode modifier_information: %v", err)
			}
		default:
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				t.Fatalf("Failed to skip field %s: %v", fieldName, err)
			}
		}
	}

	// Verify V3-specific header values
	if header.HospitalName != "West Mercy Hospital" {
		t.Errorf("Expected hospital_name 'West Mercy Hospital', got '%s'", header.HospitalName)
	}
	if header.Version != "3.0.0" {
		t.Errorf("Expected version '3.0.0', got '%s'", header.Version)
	}

	// V3 uses location_name instead of hospital_location
	if len(header.LocationName) != 2 {
		t.Errorf("Expected 2 location_name entries, got %d", len(header.LocationName))
	}
	if len(header.LocationName) > 0 && header.LocationName[0] != "West Mercy Hospital" {
		t.Errorf("Expected first location_name 'West Mercy Hospital', got '%s'", header.LocationName[0])
	}

	// V3 has type_2_npi field
	if len(header.Type2NPI) != 3 {
		t.Errorf("Expected 3 type_2_npi entries, got %d", len(header.Type2NPI))
	}
	if len(header.Type2NPI) > 0 && header.Type2NPI[0] != "0000000001" {
		t.Errorf("Expected first type_2_npi '0000000001', got '%s'", header.Type2NPI[0])
	}

	// V3 uses attestation with attester_name
	if header.Attestation.AttesterName != "Leigh Attester" {
		t.Errorf("Expected attester_name 'Leigh Attester', got '%s'", header.Attestation.AttesterName)
	}
	if !header.Attestation.ConfirmAttestation {
		t.Error("Expected confirm_attestation to be true")
	}

	// Verify standard charges
	if len(standardCharges) != 12 {
		t.Errorf("Expected 12 standard charge items in V3 file, got %d", len(standardCharges))
	}

	// Find a drug item and verify V3's numeric drug unit
	foundDrugItem := false
	for _, sci := range standardCharges {
		if sci.DrugInformation != nil {
			foundDrugItem = true
			// V3 schema has drug unit as number
			if sci.DrugInformation.Unit.Value == nil {
				t.Error("Expected drug unit to be parsed as number")
			}
			if sci.DrugInformation.Type == "" {
				t.Error("Expected drug type to be set")
			}
			break
		}
	}
	if !foundDrugItem {
		t.Error("Expected to find at least one drug item with drug_information")
	}

	// Verify V3 percentile fields in payers_information
	foundPercentileData := false
	for _, sci := range standardCharges {
		for _, sc := range sci.StandardCharges {
			for _, pi := range sc.PayersInformation {
				if pi.MedianAmount != nil && pi.Percentile10th != nil && pi.Percentile90th != nil {
					foundPercentileData = true
					break
				}
			}
		}
	}
	if !foundPercentileData {
		t.Error("Expected to find payer information with percentile data in V3 file")
	}

	// Verify modifiers
	if len(modifiers) != 3 {
		t.Errorf("Expected 3 modifiers in V3 file, got %d", len(modifiers))
	}

	// Verify V3 modifier has setting field
	if modifiers[0].Setting == nil || *modifiers[0].Setting != "both" {
		t.Errorf("Expected first modifier setting 'both', got %v", modifiers[0].Setting)
	}
}

// TestFlexibleFloatParsesStringUnit tests that FlexibleFloat correctly parses string drug units (V2 format)
func TestFlexibleFloatParsesStringUnit(t *testing.T) {
	// V2 format uses strings for drug unit
	jsonData := `{"unit": "100", "type": "ML"}`
	var di DrugInformation
	if err := json.Unmarshal([]byte(jsonData), &di); err != nil {
		t.Fatalf("Failed to unmarshal V2-style drug info with string unit: %v", err)
	}
	if di.Unit.Value == nil {
		t.Fatal("Expected unit value to be parsed")
	}
	if *di.Unit.Value != 100 {
		t.Errorf("Expected unit value 100, got %f", *di.Unit.Value)
	}
	if di.Type != "ML" {
		t.Errorf("Expected type 'ML', got '%s'", di.Type)
	}
}

// TestFlexibleFloatParsesNumericUnit tests that FlexibleFloat correctly parses numeric drug units (V3 format)
func TestFlexibleFloatParsesNumericUnit(t *testing.T) {
	// V3 format uses numbers for drug unit
	jsonData := `{"unit": 60, "type": "GM"}`
	var di DrugInformation
	if err := json.Unmarshal([]byte(jsonData), &di); err != nil {
		t.Fatalf("Failed to unmarshal V3-style drug info with numeric unit: %v", err)
	}
	if di.Unit.Value == nil {
		t.Fatal("Expected unit value to be parsed")
	}
	if *di.Unit.Value != 60 {
		t.Errorf("Expected unit value 60, got %f", *di.Unit.Value)
	}
	if di.Type != "GM" {
		t.Errorf("Expected type 'GM', got '%s'", di.Type)
	}
}

// TestFlexibleFloatParsesFormattedString tests that FlexibleFloat handles comma-formatted strings
func TestFlexibleFloatParsesFormattedString(t *testing.T) {
	jsonData := `{"unit": "24,945.00", "type": "UN"}`
	var di DrugInformation
	if err := json.Unmarshal([]byte(jsonData), &di); err != nil {
		t.Fatalf("Failed to unmarshal drug info with formatted string unit: %v", err)
	}
	if di.Unit.Value == nil {
		t.Fatal("Expected unit value to be parsed")
	}
	if *di.Unit.Value != 24945.00 {
		t.Errorf("Expected unit value 24945.00, got %f", *di.Unit.Value)
	}
}

// TestV2AffirmationVsV3Attestation tests the different attestation structures between V2 and V3
func TestV2AffirmationVsV3Attestation(t *testing.T) {
	// V2 affirmation structure
	v2JSON := `{
		"affirmation": "To the best of its knowledge...",
		"confirm_affirmation": true
	}`
	var v2Att Attestation
	if err := json.Unmarshal([]byte(v2JSON), &v2Att); err != nil {
		t.Fatalf("Failed to unmarshal V2 affirmation: %v", err)
	}
	if v2Att.Affirmation != "To the best of its knowledge..." {
		t.Errorf("V2 affirmation text not parsed correctly")
	}
	if !v2Att.ConfirmAffirmation {
		t.Error("V2 confirm_affirmation should be true")
	}

	// V3 attestation structure
	v3JSON := `{
		"attestation": "To the best of its knowledge...",
		"confirm_attestation": true,
		"attester_name": "John Doe"
	}`
	var v3Att Attestation
	if err := json.Unmarshal([]byte(v3JSON), &v3Att); err != nil {
		t.Fatalf("Failed to unmarshal V3 attestation: %v", err)
	}
	if v3Att.Attestation != "To the best of its knowledge..." {
		t.Errorf("V3 attestation text not parsed correctly")
	}
	if !v3Att.ConfirmAttestation {
		t.Error("V3 confirm_attestation should be true")
	}
	if v3Att.AttesterName != "John Doe" {
		t.Errorf("V3 attester_name expected 'John Doe', got '%s'", v3Att.AttesterName)
	}
}
