package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// NYSKeywords contains keywords to identify NYS-related plans (fallback matching)
var NYSKeywords = []string{
	"new york",
	"ny ",
	"ny-",
	"nys",
	"nyc",
	"n.y.",
	"empire",
	"healthfirst",
	"fidelis",
	"emblemhealth",
	"metroplus",
	"affinity",
	"excellus",
	"mvp health",
	"cdphp",
	"independent health",
	"univera",
}

// FilterConfig controls which plans to match
type FilterConfig struct {
	// MarketType filters by plan_market_type: "individual", "group", or "" for both
	MarketType string
	// UseHIOSStateCode uses the HIOS ID state code (positions 6-7) for matching
	// This is the most accurate method for identifying state-specific plans
	UseHIOSStateCode bool
	// StateCode is the 2-letter state code to match (default "NY")
	StateCode string
	// UseKeywords enables keyword-based matching as fallback
	UseKeywords bool
}

// DefaultFilterConfig returns the default filter configuration (no filters)
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		MarketType:       "",    // Match both individual and group
		UseHIOSStateCode: false, // No state code matching by default
		StateCode:        "",    // No state filter
		UseKeywords:      false, // No keyword matching by default
	}
}

// CurrentFilter is the active filter configuration
var CurrentFilter = DefaultFilterConfig()

// StreamParser handles streaming JSON parsing for large TOC files
type StreamParser struct {
	decoder  *json.Decoder
	stats    ParserStats
	metadata TOCMetadata
}

// ParserStats tracks parsing statistics
type ParserStats struct {
	TotalStructures   int64
	TotalPlans        int64
	MatchedStructures int64
	MatchedPlans      int64
	BytesRead         int64
}

// TOCMetadata contains the top-level TOC file metadata
type TOCMetadata struct {
	ReportingEntityName string
	ReportingEntityType string
	LastUpdatedOn       string
	Version             string
}

// NewStreamParser creates a new streaming parser
func NewStreamParser(r io.Reader) *StreamParser {
	decoder := json.NewDecoder(r)
	return &StreamParser{
		decoder: decoder,
	}
}

// isNYSPlan checks if a plan matches the current filter configuration
func isNYSPlan(plan ReportingPlan) bool {
	return matchesPlan(plan, CurrentFilter)
}

// matchesPlan checks if a plan matches the given filter configuration
func matchesPlan(plan ReportingPlan, filter FilterConfig) bool {
	// If no filters are active, match all plans
	if !filter.UseHIOSStateCode && !filter.UseKeywords && filter.MarketType == "" {
		return true
	}

	// Filter by market type if specified
	if filter.MarketType != "" && plan.PlanMarketType != filter.MarketType {
		return false
	}

	// If no state/keyword filters are active but market type matched, accept
	if !filter.UseHIOSStateCode && !filter.UseKeywords {
		return true
	}

	// Primary method: Check HIOS ID state code at positions 6-7 (0-indexed: 5-6)
	// HIOS format: [5-digit issuer][2-char state][3-digit product][optional 4-digit component]
	// Example: 12345NY0010001 - "NY" is at positions 5-6
	if filter.UseHIOSStateCode && plan.PlanIDType == "hios" {
		planID := strings.ToUpper(plan.PlanID)
		stateCode := strings.ToUpper(filter.StateCode)

		// HIOS IDs are either 10 or 14 digits, state code is at positions 5-6
		if len(planID) >= 7 {
			if planID[5:7] == stateCode {
				return true
			}
		}
	}

	// Fallback: Check plan name, issuer name, and sponsor name for keywords
	if filter.UseKeywords {
		nameLower := strings.ToLower(plan.PlanName)
		issuerLower := strings.ToLower(plan.IssuerName)
		sponsorLower := strings.ToLower(plan.PlanSponsorName)

		for _, keyword := range NYSKeywords {
			if strings.Contains(nameLower, keyword) ||
				strings.Contains(issuerLower, keyword) ||
				strings.Contains(sponsorLower, keyword) {
				return true
			}
		}
	}

	return false
}

// generateDescription creates a human-readable description for a plan
func generateDescription(plan ReportingPlan) string {
	marketDesc := "group"
	if plan.PlanMarketType == "individual" {
		marketDesc = "individual"
	}

	idTypeDesc := "EIN"
	if plan.PlanIDType == "hios" {
		idTypeDesc = "HIOS"
	}

	desc := fmt.Sprintf("%s %s market plan from %s (%s: %s)",
		plan.PlanName, marketDesc, plan.IssuerName, idTypeDesc, plan.PlanID)

	if plan.PlanSponsorName != "" {
		desc += fmt.Sprintf(", sponsored by %s", plan.PlanSponsorName)
	}

	return desc
}

// Parse streams through the TOC file and extracts NYS plans
func (p *StreamParser) Parse(onPlan func(NYSPlanOutput), onProgress func(stats ParserStats)) error {
	// Read opening brace
	t, err := p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading opening token: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object start, got %v", t)
	}

	// Read top-level fields until we hit reporting_structure
	for p.decoder.More() {
		// Read field name
		t, err := p.decoder.Token()
		if err != nil {
			return fmt.Errorf("error reading field name: %w", err)
		}

		fieldName, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected field name string, got %T", t)
		}

		switch fieldName {
		case "reporting_entity_name":
			if err := p.decoder.Decode(&p.metadata.ReportingEntityName); err != nil {
				return fmt.Errorf("error decoding reporting_entity_name: %w", err)
			}
		case "reporting_entity_type":
			if err := p.decoder.Decode(&p.metadata.ReportingEntityType); err != nil {
				return fmt.Errorf("error decoding reporting_entity_type: %w", err)
			}
		case "last_updated_on":
			if err := p.decoder.Decode(&p.metadata.LastUpdatedOn); err != nil {
				return fmt.Errorf("error decoding last_updated_on: %w", err)
			}
		case "version":
			if err := p.decoder.Decode(&p.metadata.Version); err != nil {
				return fmt.Errorf("error decoding version: %w", err)
			}
		case "reporting_structure":
			// Stream through the reporting_structure array
			if err := p.parseReportingStructure(onPlan, onProgress); err != nil {
				return err
			}
		default:
			// Skip unknown fields
			var skip json.RawMessage
			if err := p.decoder.Decode(&skip); err != nil {
				return fmt.Errorf("error skipping field %s: %w", fieldName, err)
			}
		}
	}

	return nil
}

// parseReportingStructure streams through the reporting_structure array
func (p *StreamParser) parseReportingStructure(onPlan func(NYSPlanOutput), onProgress func(stats ParserStats)) error {
	// Read opening bracket of array
	t, err := p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading reporting_structure start: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected array start for reporting_structure, got %v", t)
	}

	// Stream through each reporting structure
	for p.decoder.More() {
		var rs ReportingStructure
		if err := p.decoder.Decode(&rs); err != nil {
			return fmt.Errorf("error decoding reporting structure: %w", err)
		}

		p.stats.TotalStructures++
		p.stats.TotalPlans += int64(len(rs.ReportingPlans))

		// Check if any plan in this structure is NYS-related
		hasNYSPlan := false
		for _, plan := range rs.ReportingPlans {
			if isNYSPlan(plan) {
				hasNYSPlan = true
				break
			}
		}

		if hasNYSPlan {
			p.stats.MatchedStructures++

			// Extract in-network URLs
			var urls []string
			for _, f := range rs.InNetworkFiles {
				urls = append(urls, f.Location)
			}

			// Output each NYS plan with the associated URLs
			for _, plan := range rs.ReportingPlans {
				if isNYSPlan(plan) {
					p.stats.MatchedPlans++

					output := NYSPlanOutput{
						PlanName:       plan.PlanName,
						PlanIDType:     plan.PlanIDType,
						PlanID:         plan.PlanID,
						PlanMarketType: plan.PlanMarketType,
						IssuerName:     plan.IssuerName,
						Description:    generateDescription(plan),
						InNetworkURLs:  urls,
					}
					onPlan(output)
				}
			}
		}

		// Report progress periodically
		if p.stats.TotalStructures%10000 == 0 {
			onProgress(p.stats)
		}
	}

	// Read closing bracket
	_, err = p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading reporting_structure end: %w", err)
	}

	return nil
}

// GetStats returns current parsing statistics
func (p *StreamParser) GetStats() ParserStats {
	return p.stats
}

// GetMetadata returns the TOC file metadata
func (p *StreamParser) GetMetadata() TOCMetadata {
	return p.metadata
}
