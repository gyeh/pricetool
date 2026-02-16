package main

// RateRow is the Parquet schema for denormalized negotiated rates.
// One row per negotiated price, with all parent metadata denormalized.
type RateRow struct {
	ReportingEntityName    string   `parquet:"reporting_entity_name"`
	ReportingEntityType    string   `parquet:"reporting_entity_type"`
	PlanName               *string  `parquet:"plan_name,optional"`
	IssuerName             *string  `parquet:"issuer_name,optional"`
	PlanSponsorName        *string  `parquet:"plan_sponsor_name,optional"`
	PlanIDType             *string  `parquet:"plan_id_type,optional"`
	PlanID                 *string  `parquet:"plan_id,optional"`
	PlanMarketType         *string  `parquet:"plan_market_type,optional"`
	LastUpdatedOn          string   `parquet:"last_updated_on"`
	Version                string   `parquet:"version"`
	NegotiationArrangement string   `parquet:"negotiation_arrangement"`
	Name                   string   `parquet:"name"`
	BillingCodeType        string   `parquet:"billing_code_type"`
	BillingCodeTypeVersion string   `parquet:"billing_code_type_version"`
	BillingCode            string   `parquet:"billing_code"`
	Description            string   `parquet:"description"`
	NegotiatedRate         float64  `parquet:"negotiated_rate"`
	NegotiatedType         string   `parquet:"negotiated_type"`
	BillingClass           string   `parquet:"billing_class"`
	Setting                string   `parquet:"setting"`
	ExpirationDate         string   `parquet:"expiration_date"`
	ServiceCode            []string `parquet:"service_code,list,optional"`
	BillingCodeModifier    []string `parquet:"billing_code_modifier,list,optional"`
	AdditionalInformation  *string  `parquet:"additional_information,optional"`
	ProviderGroupIDs       []int32  `parquet:"provider_group_ids,list,optional"`
	BundledCodesJSON       *string  `parquet:"bundled_codes_json,optional"`
	CoveredServicesJSON    *string  `parquet:"covered_services_json,optional"`
}

// ProviderRow is the Parquet schema for provider reference data.
// One row per (provider_group_id, NPI) combination.
type ProviderRow struct {
	ProviderGroupID int32    `parquet:"provider_group_id"`
	NPI             int64    `parquet:"npi"`
	TINType         string   `parquet:"tin_type"`
	TINValue        string   `parquet:"tin_value"`
	BusinessName    *string  `parquet:"business_name,optional"`
	NetworkNames    []string `parquet:"network_names,list,optional"`
}
