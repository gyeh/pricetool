package main

import (
	"strings"
	"testing"
)

// nysFilter returns a filter configured for NY state matching
func nysFilter() FilterConfig {
	return FilterConfig{
		MarketType:       "",
		UseHIOSStateCode: true,
		StateCode:        "NY",
		UseKeywords:      true,
	}
}

func TestIsNYSPlan(t *testing.T) {
	// Set filter to NY for this test
	CurrentFilter = nysFilter()

	tests := []struct {
		name     string
		plan     ReportingPlan
		expected bool
	}{
		{
			name: "HIOS with NY state code",
			plan: ReportingPlan{
				PlanName:       "Gold Plan",
				IssuerName:     "Some Issuer",
				PlanIDType:     "hios",
				PlanID:         "12345NY0010001", // NY at positions 5-6
				PlanMarketType: "individual",
			},
			expected: true,
		},
		{
			name: "HIOS with NY state code (10-digit)",
			plan: ReportingPlan{
				PlanName:       "Silver Plan",
				IssuerName:     "Some Issuer",
				PlanIDType:     "hios",
				PlanID:         "12345NY001", // NY at positions 5-6
				PlanMarketType: "individual",
			},
			expected: true,
		},
		{
			name: "HIOS with CA state code (should not match)",
			plan: ReportingPlan{
				PlanName:       "California Plan",
				IssuerName:     "Kaiser Permanente",
				PlanIDType:     "hios",
				PlanID:         "12345CA001",
				PlanMarketType: "individual",
			},
			expected: false,
		},
		{
			name: "New York in issuer name (keyword match)",
			plan: ReportingPlan{
				PlanName:       "Basic Health Plan",
				IssuerName:     "New York Health Insurance",
				PlanIDType:     "hios",
				PlanID:         "12345XX001", // Not NY in HIOS
				PlanMarketType: "individual",
			},
			expected: true,
		},
		{
			name: "Healthfirst keyword",
			plan: ReportingPlan{
				PlanName:       "Essential Plan",
				IssuerName:     "Healthfirst",
				PlanIDType:     "hios",
				PlanID:         "12345XX001",
				PlanMarketType: "individual",
			},
			expected: true,
		},
		{
			name: "Empire keyword",
			plan: ReportingPlan{
				PlanName:       "Empire Blue Cross",
				IssuerName:     "Anthem",
				PlanIDType:     "hios",
				PlanID:         "12345XX001",
				PlanMarketType: "group",
			},
			expected: true,
		},
		{
			name: "Non-NYS plan with no matches",
			plan: ReportingPlan{
				PlanName:       "Generic Health Plan",
				IssuerName:     "Generic Insurer",
				PlanIDType:     "hios",
				PlanID:         "99999TX001",
				PlanMarketType: "individual",
			},
			expected: false,
		},
		{
			name: "EIN plan with NY keyword",
			plan: ReportingPlan{
				PlanName:       "Brooklyn Health Partners",
				IssuerName:     "Local Insurer",
				PlanIDType:     "ein",
				PlanID:         "123456789",
				PlanMarketType: "group",
			},
			expected: false, // Brooklyn was removed from keywords
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNYSPlan(tt.plan)
			if result != tt.expected {
				t.Errorf("isNYSPlan(%+v) = %v, expected %v", tt.plan, result, tt.expected)
			}
		})
	}
}

func TestMatchesPlanWithMarketTypeFilter(t *testing.T) {
	plan := ReportingPlan{
		PlanName:       "Gold Plan",
		IssuerName:     "Some Issuer",
		PlanIDType:     "hios",
		PlanID:         "12345NY001",
		PlanMarketType: "individual",
	}

	// Test with individual market filter
	filter := FilterConfig{
		MarketType:       "individual",
		UseHIOSStateCode: true,
		StateCode:        "NY",
		UseKeywords:      true,
	}
	if !matchesPlan(plan, filter) {
		t.Error("Should match individual market plan with individual filter")
	}

	// Test with group market filter (should not match)
	filter.MarketType = "group"
	if matchesPlan(plan, filter) {
		t.Error("Should not match individual plan with group filter")
	}

	// Test with empty market filter (matches all)
	filter.MarketType = ""
	if !matchesPlan(plan, filter) {
		t.Error("Should match with empty market filter")
	}
}

func TestMatchesPlanWithDifferentStates(t *testing.T) {
	tests := []struct {
		name      string
		planID    string
		stateCode string
		expected  bool
	}{
		{"NY plan with NY filter", "12345NY001", "NY", true},
		{"NY plan with CA filter", "12345NY001", "CA", false},
		{"CA plan with CA filter", "12345CA001", "CA", true},
		{"TX plan with TX filter", "12345TX001", "TX", true},
		{"FL plan with NY filter", "12345FL001", "NY", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := ReportingPlan{
				PlanName:       "Test Plan",
				IssuerName:     "Test Issuer",
				PlanIDType:     "hios",
				PlanID:         tt.planID,
				PlanMarketType: "individual",
			}
			filter := FilterConfig{
				UseHIOSStateCode: true,
				StateCode:        tt.stateCode,
				UseKeywords:      false, // Disable keywords to test HIOS only
			}
			result := matchesPlan(plan, filter)
			if result != tt.expected {
				t.Errorf("matchesPlan with state %s = %v, expected %v", tt.stateCode, result, tt.expected)
			}
		})
	}
}

func TestGenerateDescription(t *testing.T) {
	plan := ReportingPlan{
		PlanName:       "Gold Health Plan",
		IssuerName:     "NY Health Insurance",
		PlanIDType:     "hios",
		PlanID:         "12345NY001",
		PlanMarketType: "individual",
	}

	desc := generateDescription(plan)

	if !strings.Contains(desc, "Gold Health Plan") {
		t.Error("Description should contain plan name")
	}
	if !strings.Contains(desc, "individual") {
		t.Error("Description should contain market type")
	}
	if !strings.Contains(desc, "NY Health Insurance") {
		t.Error("Description should contain issuer name")
	}
	if !strings.Contains(desc, "HIOS") {
		t.Error("Description should contain ID type")
	}
}

func TestStreamParser(t *testing.T) {
	// Set filter to NY for this test
	CurrentFilter = nysFilter()

	tocJSON := `{
		"reporting_entity_name": "Test Entity",
		"reporting_entity_type": "health_insurance_issuer",
		"last_updated_on": "2024-01-01",
		"version": "2.0.0",
		"reporting_structure": [
			{
				"reporting_plans": [
					{
						"plan_name": "NY Essential Plan",
						"plan_id_type": "hios",
						"plan_id": "12345NY001",
						"plan_market_type": "individual",
						"issuer_name": "EmblemHealth"
					}
				],
				"in_network_files": [
					{
						"description": "In-network rates",
						"location": "https://example.com/in-network.json"
					}
				]
			},
			{
				"reporting_plans": [
					{
						"plan_name": "California Plan",
						"plan_id_type": "hios",
						"plan_id": "12345CA001",
						"plan_market_type": "individual",
						"issuer_name": "Kaiser"
					}
				],
				"in_network_files": [
					{
						"description": "CA rates",
						"location": "https://example.com/ca-network.json"
					}
				]
			},
			{
				"reporting_plans": [
					{
						"plan_name": "Healthfirst Bronze",
						"plan_id_type": "hios",
						"plan_id": "68804NY002",
						"plan_market_type": "group",
						"issuer_name": "Healthfirst"
					}
				],
				"in_network_files": [
					{
						"description": "HF rates",
						"location": "https://example.com/hf-network.json"
					},
					{
						"description": "HF behavioral",
						"location": "https://example.com/hf-behavioral.json"
					}
				]
			}
		]
	}`

	reader := strings.NewReader(tocJSON)
	parser := NewStreamParser(reader)

	var plans []NYSPlanOutput
	onPlan := func(plan NYSPlanOutput) {
		plans = append(plans, plan)
	}
	onProgress := func(stats ParserStats) {}

	err := parser.Parse(onPlan, onProgress)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	stats := parser.GetStats()

	// Should have 3 total structures
	if stats.TotalStructures != 3 {
		t.Errorf("Expected 3 total structures, got %d", stats.TotalStructures)
	}

	// Should have 2 matched structures (NY Essential and Healthfirst with NY HIOS)
	if stats.MatchedStructures != 2 {
		t.Errorf("Expected 2 matched structures, got %d", stats.MatchedStructures)
	}

	// Should have 2 matched plans
	if len(plans) != 2 {
		t.Errorf("Expected 2 matched plans, got %d", len(plans))
	}

	// Verify first plan
	if len(plans) > 0 && plans[0].PlanName != "NY Essential Plan" {
		t.Errorf("Expected first plan to be 'NY Essential Plan', got '%s'", plans[0].PlanName)
	}
	if len(plans) > 0 && len(plans[0].InNetworkURLs) != 1 {
		t.Errorf("Expected 1 URL for first plan, got %d", len(plans[0].InNetworkURLs))
	}

	// Verify second plan has 2 URLs
	if len(plans) > 1 && plans[1].PlanName != "Healthfirst Bronze" {
		t.Errorf("Expected second plan to be 'Healthfirst Bronze', got '%s'", plans[1].PlanName)
	}
	if len(plans) > 1 && len(plans[1].InNetworkURLs) != 2 {
		t.Errorf("Expected 2 URLs for second plan, got %d", len(plans[1].InNetworkURLs))
	}
}

func TestStreamParserWithSampleFile(t *testing.T) {
	// Set filter to NY for this test
	CurrentFilter = nysFilter()

	// Test with the actual sample file structure
	sampleJSON := `{
		"reporting_entity_name": "medicare",
		"reporting_entity_type": "medicare",
		"last_updated_on": "2023-04-19",
		"version": "2.0.0",
		"reporting_structure": [
			{
				"reporting_plans": [
					{
						"plan_name": "medicaid",
						"plan_id_type": "hios",
						"issuer_name": "CMS",
						"plan_id": "1111111111",
						"plan_market_type": "individual"
					},
					{
						"plan_name": "medicare",
						"plan_id_type": "hios",
						"issuer_name": "CMS",
						"plan_id": "0000000000",
						"plan_market_type": "individual"
					}
				],
				"in_network_files": [
					{
						"description": "in-network file",
						"location": "https://www.some_site.com/files/in-network-file-123456.json"
					}
				]
			}
		]
	}`

	reader := strings.NewReader(sampleJSON)
	parser := NewStreamParser(reader)

	var plans []NYSPlanOutput
	onPlan := func(plan NYSPlanOutput) {
		plans = append(plans, plan)
	}
	onProgress := func(stats ParserStats) {}

	err := parser.Parse(onPlan, onProgress)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	stats := parser.GetStats()
	if stats.TotalStructures != 1 {
		t.Errorf("Expected 1 structure, got %d", stats.TotalStructures)
	}
	if stats.TotalPlans != 2 {
		t.Errorf("Expected 2 total plans, got %d", stats.TotalPlans)
	}
	// Neither medicaid nor medicare have NY in HIOS position or match keywords
	if len(plans) != 0 {
		t.Errorf("Expected 0 NYS plans from sample, got %d", len(plans))
	}
}
