package main

// RootMetadata holds the top-level fields from an in-network rates file.
type RootMetadata struct {
	ReportingEntityName string
	ReportingEntityType string
	PlanName            *string
	IssuerName          *string
	PlanSponsorName     *string
	PlanIDType          *string
	PlanID              *string
	PlanMarketType      *string
	LastUpdatedOn       string
	Version             string
}

// ProviderReference is a top-level provider reference entry.
type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	NetworkName     []string        `json:"network_name"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
}

// ProviderGroup contains a TIN and list of NPIs.
type ProviderGroup struct {
	NPI []int64 `json:"npi"`
	TIN TIN     `json:"tin"`
}

// TIN contains tax identification number details.
type TIN struct {
	Type         string `json:"type"`
	Value        string `json:"value"`
	BusinessName string `json:"business_name"`
}

// InNetworkItem represents a single in-network service/procedure.
type InNetworkItem struct {
	NegotiationArrangement string           `json:"negotiation_arrangement"`
	Name                   string           `json:"name"`
	BillingCodeType        string           `json:"billing_code_type"`
	BillingCodeTypeVersion string           `json:"billing_code_type_version"`
	BillingCode            string           `json:"billing_code"`
	Description            string           `json:"description"`
	NegotiatedRates        []NegotiatedRate `json:"negotiated_rates"`
	BundledCodes           []ContainedCode  `json:"bundled_codes"`
	CoveredServices        []ContainedCode  `json:"covered_services"`
}

// NegotiatedRate groups negotiated prices with provider references.
// Supports two CMS formats:
//   - Referenced: provider_references contains integer IDs pointing to top-level provider_references
//   - Embedded: provider_groups contains provider data directly inline
type NegotiatedRate struct {
	ProviderReferences []int             `json:"provider_references"`
	ProviderGroups     []ProviderGroup   `json:"provider_groups"`
	NegotiatedPrices   []NegotiatedPrice `json:"negotiated_prices"`
}

// NegotiatedPrice contains a single negotiated price.
type NegotiatedPrice struct {
	NegotiatedType        string   `json:"negotiated_type"`
	NegotiatedRate        float64  `json:"negotiated_rate"`
	BillingClass          string   `json:"billing_class"`
	Setting               string   `json:"setting"`
	ExpirationDate        string   `json:"expiration_date"`
	ServiceCode           []string `json:"service_code"`
	BillingCodeModifier   []string `json:"billing_code_modifier"`
	AdditionalInformation string   `json:"additional_information"`
}

// ContainedCode is used in bundled_codes and covered_services.
type ContainedCode struct {
	BillingCodeType        string `json:"billing_code_type"`
	BillingCodeTypeVersion string `json:"billing_code_type_version"`
	BillingCode            string `json:"billing_code"`
	Description            string `json:"description"`
}
