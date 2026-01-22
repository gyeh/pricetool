package main

import (
	"encoding/json"
	"strconv"
	"strings"
)

// FlexibleFloat handles both string and number JSON values
type FlexibleFloat struct {
	Value *float64
}

func (f *FlexibleFloat) UnmarshalJSON(data []byte) error {
	// Try as number first
	var num float64
	if err := json.Unmarshal(data, &num); err == nil {
		f.Value = &num
		return nil
	}
	// Try as string (e.g., "24,945.00")
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		// Remove commas and parse
		cleaned := strings.ReplaceAll(str, ",", "")
		if cleaned == "" {
			f.Value = nil
			return nil
		}
		num, err := strconv.ParseFloat(cleaned, 64)
		if err != nil {
			return err
		}
		f.Value = &num
		return nil
	}
	// null
	f.Value = nil
	return nil
}

// CodeInformation represents a billing code
type CodeInformation struct {
	Code string `json:"code"`
	Type string `json:"type"`
}

// DrugInformation contains drug unit details
type DrugInformation struct {
	Unit FlexibleFloat `json:"unit"`
	Type string        `json:"type"`
}

// PayerInformation contains payer-specific charge details
type PayerInformation struct {
	PayerName                string   `json:"payer_name"`
	PlanName                 string   `json:"plan_name"`
	AdditionalPayerNotes     *string  `json:"additional_payer_notes,omitempty"`
	StandardChargeDollar     *float64 `json:"standard_charge_dollar,omitempty"`
	StandardChargeAlgorithm  *string  `json:"standard_charge_algorithm,omitempty"`
	StandardChargePercentage *float64 `json:"standard_charge_percentage,omitempty"`
	EstimatedAmount          *float64 `json:"estimated_amount,omitempty"` // V2
	MedianAmount             *float64 `json:"median_amount,omitempty"`
	Percentile10th           *float64 `json:"10th_percentile,omitempty"`
	Percentile90th           *float64 `json:"90th_percentile,omitempty"`
	Count                    *string  `json:"count,omitempty"`
	Methodology              string   `json:"methodology"`
}

// StandardCharge represents a charge for a specific setting
type StandardCharge struct {
	Minimum                *float64           `json:"minimum,omitempty"`
	Maximum                *float64           `json:"maximum,omitempty"`
	GrossCharge            *float64           `json:"gross_charge,omitempty"`
	GrossCharges           *FlexibleFloat     `json:"gross_charges,omitempty"` // V2 uses string format
	DiscountedCash         *float64           `json:"discounted_cash,omitempty"`
	Setting                string             `json:"setting"`
	ModifierCode           []string           `json:"modifier_code,omitempty"`
	PayersInformation      []PayerInformation `json:"payers_information,omitempty"`
	AdditionalGenericNotes *string            `json:"additional_generic_notes,omitempty"`
}

// StandardChargeInformation represents a billable item with its charges
type StandardChargeInformation struct {
	Description     string            `json:"description"`
	DrugInformation *DrugInformation  `json:"drug_information,omitempty"`
	CodeInformation []CodeInformation `json:"code_information"`
	StandardCharges []StandardCharge  `json:"standard_charges"`
}

// ModifierPayerInformation contains payer-specific modifier details
type ModifierPayerInformation struct {
	PayerName   string `json:"payer_name"`
	PlanName    string `json:"plan_name"`
	Description string `json:"description"`
}

// ModifierInformation represents a billing modifier
type ModifierInformation struct {
	Description              string                     `json:"description"`
	Code                     string                     `json:"code"`
	Setting                  *string                    `json:"setting,omitempty"`
	ModifierPayerInformation []ModifierPayerInformation `json:"modifier_payer_information"`
}

// Attestation contains attestation/affirmation details (V2 uses affirmation, V3 uses attestation)
type Attestation struct {
	Attestation        string `json:"attestation"`
	Affirmation        string `json:"affirmation"` // V2 uses "affirmation" instead of "attestation"
	ConfirmAttestation bool   `json:"confirm_attestation"`
	ConfirmAffirmation bool   `json:"confirm_affirmation"` // V2
	AttesterName       string `json:"attester_name"`
}

// LicenseInformation contains hospital license details
type LicenseInformation struct {
	LicenseNumber *string `json:"license_number,omitempty"`
	State         string  `json:"state"`
}

// HospitalHeader contains only the small header fields (not the large arrays)
type HospitalHeader struct {
	HospitalName       string             `json:"hospital_name"`
	HospitalAddress    []string           `json:"hospital_address"`
	LastUpdatedOn      string             `json:"last_updated_on"`
	Attestation        Attestation        `json:"attestation"`
	Affirmation        Attestation        `json:"affirmation"` // V2 uses "affirmation" instead of "attestation"
	LicenseInformation LicenseInformation `json:"license_information"`
	Version            string             `json:"version"`
	LocationName       []string           `json:"location_name"`
	HospitalLocation   []string           `json:"hospital_location"` // V2 uses "hospital_location"
	Type2NPI           []string           `json:"type_2_npi"`
}
