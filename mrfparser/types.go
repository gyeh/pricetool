package main

// TOC file types for MRF (Machine-Readable Files) parsing
// Based on CMS Price Transparency Guide schema

// TOCFile represents the top-level Table of Contents structure
type TOCFile struct {
	ReportingEntityName string               `json:"reporting_entity_name"`
	ReportingEntityType string               `json:"reporting_entity_type"`
	LastUpdatedOn       string               `json:"last_updated_on"`
	Version             string               `json:"version"`
	ReportingStructure  []ReportingStructure `json:"reporting_structure"`
}

// ReportingStructure maps plans to their in-network and allowed amount files
type ReportingStructure struct {
	ReportingPlans    []ReportingPlan `json:"reporting_plans"`
	InNetworkFiles    []FileLocation  `json:"in_network_files,omitempty"`
	AllowedAmountFile *FileLocation   `json:"allowed_amount_file,omitempty"`
}

// ReportingPlan contains plan information
type ReportingPlan struct {
	PlanName        string `json:"plan_name"`
	IssuerName      string `json:"issuer_name"`
	PlanIDType      string `json:"plan_id_type"`   // "ein" or "hios"
	PlanID          string `json:"plan_id"`
	PlanSponsorName string `json:"plan_sponsor_name,omitempty"`
	PlanMarketType  string `json:"plan_market_type"` // "group" or "individual"
}

// FileLocation contains file description and URL
type FileLocation struct {
	Description string `json:"description"`
	Location    string `json:"location"`
}

// NYSPlanOutput is the output format for extracted NYS plans
type NYSPlanOutput struct {
	PlanName       string   `json:"plan_name"`
	PlanIDType     string   `json:"plan_id_type"`
	PlanID         string   `json:"plan_id"`
	PlanMarketType string   `json:"plan_market_type"`
	IssuerName     string   `json:"issuer_name"`
	Description    string   `json:"description"`
	InNetworkURLs  []string `json:"in_network_urls"`
}

// OutputFile is the complete output structure
type OutputFile struct {
	ReportingEntityName string          `json:"reporting_entity_name"`
	ReportingEntityType string          `json:"reporting_entity_type"`
	LastUpdatedOn       string          `json:"last_updated_on"`
	ExtractedAt         string          `json:"extracted_at"`
	TotalPlansExtracted int             `json:"total_plans_extracted"`
	Plans               []NYSPlanOutput `json:"plans"`
}
