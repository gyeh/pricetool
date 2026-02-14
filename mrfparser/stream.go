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
		if err := p.parseOneStructure(onPlan); err != nil {
			return err
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

// parseOneStructure streams through a single reporting structure object,
// decoding plans one at a time to limit per-structure memory usage.
// Handles either field ordering (reporting_plans before/after in_network_files).
func (p *StreamParser) parseOneStructure(onPlan func(NYSPlanOutput)) error {
	// Read opening brace
	t, err := p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading structure start: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object start for structure, got %v", t)
	}

	p.stats.TotalStructures++

	var urls []string
	var pendingPlans []ReportingPlan
	urlsResolved := false
	matchedInStructure := 0

	emitPlan := func(plan ReportingPlan) {
		matchedInStructure++
		p.stats.MatchedPlans++
		onPlan(NYSPlanOutput{
			PlanName:       plan.PlanName,
			PlanIDType:     plan.PlanIDType,
			PlanID:         plan.PlanID,
			PlanMarketType: plan.PlanMarketType,
			IssuerName:     plan.IssuerName,
			Description:    generateDescription(plan),
			InNetworkURLs:  urls,
			StructureID:    p.stats.TotalStructures,
		})
	}

	for p.decoder.More() {
		t, err := p.decoder.Token()
		if err != nil {
			return fmt.Errorf("error reading structure field: %w", err)
		}
		fieldName, ok := t.(string)
		if !ok {
			return fmt.Errorf("expected field name, got %T", t)
		}

		switch fieldName {
		case "reporting_plans":
			if err := p.streamArray(func() error {
				var plan ReportingPlan
				if err := p.decoder.Decode(&plan); err != nil {
					return fmt.Errorf("error decoding plan: %w", err)
				}

				p.stats.TotalPlans++
				if !isNYSPlan(plan) {
					return nil
				}
				if urlsResolved {
					emitPlan(plan)
				} else {
					// Buffer matched plans until we have URLs
					pendingPlans = append(pendingPlans, plan)
				}
				return nil
			}); err != nil {
				return err
			}

		case "in_network_files":
			if err := p.streamArray(func() error {
				var f FileLocation
				if err := p.decoder.Decode(&f); err != nil {
					return fmt.Errorf("error decoding file location: %w", err)
				}
				urls = append(urls, f.Location)
				return nil
			}); err != nil {
				return err
			}
			urlsResolved = true

			// Flush plans that were buffered before URLs were known
			for _, plan := range pendingPlans {
				emitPlan(plan)
			}
			pendingPlans = nil

		default:
			var skip json.RawMessage
			if err := p.decoder.Decode(&skip); err != nil {
				return fmt.Errorf("error skipping field %s: %w", fieldName, err)
			}
		}
	}

	// Emit any remaining buffered plans (e.g. no in_network_files field)
	for _, plan := range pendingPlans {
		emitPlan(plan)
	}

	if matchedInStructure > 0 {
		p.stats.MatchedStructures++
	}

	// Read closing brace
	_, err = p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading structure end: %w", err)
	}

	return nil
}

// streamArray reads a JSON array token by token, calling fn for each element.
// fn must consume exactly one element from the decoder per call.
func (p *StreamParser) streamArray(fn func() error) error {
	t, err := p.decoder.Token()
	if err != nil {
		return fmt.Errorf("error reading array start: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected array start, got %v", t)
	}

	for p.decoder.More() {
		if err := fn(); err != nil {
			return err
		}
	}

	// Read closing bracket
	_, err = p.decoder.Token()
	return err
}

// GetStats returns current parsing statistics
func (p *StreamParser) GetStats() ParserStats {
	return p.stats
}

// GetMetadata returns the TOC file metadata
func (p *StreamParser) GetMetadata() TOCMetadata {
	return p.metadata
}
