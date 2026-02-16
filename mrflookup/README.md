# MRF Plan Lookup Service

A Go package and tools for mapping consumer health plan information to MRF (Machine Readable File) plan IDs.

## The Problem

Consumers have insurance cards with:
- Member ID
- Group Number
- Plan Name (marketing name)
- Insurance company name

MRF files use:
- HIOS ID (10-14 digit identifier)
- EIN (for employer-sponsored plans)
- Plan names (often different from marketing names)

**There's no direct mapping between what consumers see and what's in MRF files.**

## The Solution

This package provides a consolidated lookup service that:

1. **Loads multiple data sources:**
   - CMS Plan Attributes PUF (Public Use Files)
   - MRF Table of Contents files
   - Issuer aliases
   - EIN mappings for employers

2. **Indexes data for fast lookups**

3. **Scores potential matches using:**
   - Fuzzy name matching
   - State filtering
   - Metal level matching (Bronze/Silver/Gold/Platinum)
   - Plan type matching (HMO/PPO/EPO/POS)
   - EIN matching for employer plans

## Installation

```bash
# Clone or copy the package
cp -r mrflookup /path/to/your/project/

# Build the CLI
cd mrflookup/cmd/mrflookup-cli
go build -o mrflookup-cli

# Build the HTTP server
cd ../mrflookup-server
go build -o mrflookup-server
```

## Quick Start

### As a Go Package

```go
package main

import (
    "fmt"
    "mrflookup"
)

func main() {
    // Create lookup service
    lookup := mrflookup.NewLookupService()
    
    // Load data sources
    lookup.LoadPlanAttributesPUF("plan_attributes.csv")
    lookup.LoadMRFTableOfContents("toc_index.json")
    
    // Search for plans
    results := lookup.FindPlans(mrflookup.ConsumerInput{
        IssuerName: "Empire Blue Cross",
        PlanName:   "Gold HMO",
        State:      "NY",
        MetalLevel: "Gold",
    })
    
    for _, r := range results {
        fmt.Printf("Plan: %s (ID: %s, Score: %.1f%%)\n", 
            r.PlanName, r.PlanID, r.MatchScore)
    }
}
```

### CLI Tool

```bash
# Direct query
./mrflookup-cli \
    --puf plan_attributes.csv \
    --toc toc_index.json \
    --issuer "Oscar" \
    --state NY \
    --metal Silver

# Interactive mode
./mrflookup-cli \
    --puf plan_attributes.csv \
    --toc toc_index.json \
    --interactive

# JSON output
./mrflookup-cli \
    --puf plan_attributes.csv \
    --toc toc_index.json \
    --issuer "Fidelis" \
    --state NY \
    --json
```

### HTTP Server

```bash
# Start the server
./mrflookup-server \
    --puf plan_attributes.csv \
    --toc toc_index.json \
    --port 8080

# Query via API
curl -X POST http://localhost:8080/api/v1/lookup \
    -H "Content-Type: application/json" \
    -d '{
        "issuer_name": "Empire Blue Cross",
        "plan_name": "Gold HMO",
        "state": "NY"
    }'
```

## Data Sources

### 1. CMS Plan Attributes PUF (Recommended)

Download from: https://www.cms.gov/marketplace/resources/data/public-use-files

This provides the most comprehensive mapping for ACA marketplace plans, including:
- 14-digit HIOS IDs
- Plan marketing names
- Issuer names
- Metal levels
- Plan types (HMO/PPO/etc.)
- Network information

```bash
# Load PUF file
lookup.LoadPlanAttributesPUF("PlanAttributes_PY2024.csv")
```

### 2. MRF Table of Contents Files

Download from individual payer websites (required by CMS transparency rules).

Major payers:
- **UnitedHealthcare**: https://transparency-in-coverage.uhc.com/
- **Anthem/Empire**: https://www.anthem.com/machine-readable-file/search
- **Aetna**: https://health1.aetna.com/app/public/#/one/insurerCode=AETNA_I&brandCode=ALICSI
- **Cigna**: https://www.cigna.com/legal/compliance/machine-readable-files
- **Humana**: https://developers.humana.com/syntheticdata/Resource/PCTFilesList

```bash
# Load single TOC
lookup.LoadMRFTableOfContents("uhc_toc_index.json")

# Load multiple TOCs (comma-separated)
./mrflookup-cli --toc "uhc_toc.json,anthem_toc.json,aetna_toc.json"

# Stream large files (>100MB automatically uses streaming)
lookup.LoadMRFTableOfContentsStreaming("large_toc.json")
```

### 3. Issuer Aliases (Optional but Recommended)

Maps common names to HIOS issuer IDs:

```json
{
  "12345": ["Empire Blue Cross", "Empire BCBS", "Anthem NY"],
  "56789": ["Oscar", "Oscar Health", "Oscar Health Plan"]
}
```

### 4. EIN Mappings (For Employer Plans)

Maps employer names to EINs:

```json
{
  "Acme Corporation": "12-3456789",
  "Big Tech Inc": "98-7654321"
}
```

## Match Scoring

Results are scored 0-100 based on weighted criteria:

| Criterion | Weight | Notes |
|-----------|--------|-------|
| State Match | 15 | **Required filter** - no results if mismatch |
| Exact Plan Name | 40 | Full string match |
| Fuzzy Plan Name | 25 | Token-based similarity |
| Issuer Name | 25 | Contains or fuzzy match |
| Metal Level | 10 | Bronze/Silver/Gold/Platinum |
| Plan Type | 10 | HMO/PPO/EPO/POS |
| EIN Match | 50 | For employer plans (very high confidence) |

Configure weights:

```go
config := mrflookup.DefaultConfig()
config.Weights.ExactPlanName = 50.0  // Increase importance
config.Weights.FuzzyPlanName = 20.0
config.MinMatchScore = 40.0          // Require higher confidence
config.MaxResults = 10

lookup := mrflookup.NewLookupServiceWithConfig(config)
```

## API Reference

### LookupService

```go
// Create service
lookup := mrflookup.NewLookupService()
lookup := mrflookup.NewLookupServiceWithConfig(config)

// Load data
lookup.LoadPlanAttributesPUF(filename string) error
lookup.LoadMRFTableOfContents(filename string) error
lookup.LoadMRFTableOfContentsStreaming(filename string) error
lookup.LoadIssuerAliases(aliases map[string][]string)
lookup.LoadEINMappings(mappings map[string]string)
lookup.AddPlan(plan *PlanRecord)

// Query
results := lookup.FindPlans(input ConsumerInput) []MatchResult
stats := lookup.GetStats() map[string]int
```

### ConsumerInput

```go
type ConsumerInput struct {
    IssuerName   string // Insurance company name
    PlanName     string // Plan name from card
    State        string // 2-letter state code
    MetalLevel   string // Bronze/Silver/Gold/Platinum
    PlanType     string // HMO/PPO/EPO/POS
    EmployerName string // For employer plans
    EmployerEIN  string // If known
    GroupNumber  string // From insurance card
}
```

### MatchResult

```go
type MatchResult struct {
    PlanID         string            // HIOS ID or EIN
    PlanIDType     string            // "hios" or "ein"
    PlanName       string            // Marketing name
    IssuerName     string            
    IssuerID       string            // 5-digit HIOS issuer ID
    State          string            
    MarketType     string            // "individual" or "group"
    MetalLevel     string            
    PlanType       string            
    NetworkURL     string            
    InNetworkFiles []FileLocation    // MRF file URLs
    MatchScore     float64           // 0-100
    MatchDetails   map[string]string // Explains scoring
    MatchType      string            // "exact", "fuzzy", "partial"
}
```

## HTTP API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/lookup` | Search for plans |
| GET | `/api/v1/stats` | Get data statistics |
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/plan/{id}` | Get plan by HIOS ID |

### POST /api/v1/lookup

Request:
```json
{
  "issuer_name": "Empire Blue Cross",
  "plan_name": "Gold HMO",
  "state": "NY",
  "metal_level": "Gold",
  "plan_type": "HMO",
  "max_results": 10,
  "min_score": 50
}
```

Response:
```json
{
  "success": true,
  "count": 2,
  "results": [
    {
      "plan_id": "12345NY0010001",
      "plan_id_type": "hios",
      "plan_name": "Empire Gold HMO 1500",
      "issuer_name": "Empire HealthChoice HMO Inc",
      "state": "NY",
      "match_score": 87.5,
      "in_network_files": [
        {
          "description": "In-network rates",
          "location": "https://..."
        }
      ]
    }
  ],
  "timing": "2.5ms"
}
```

## Performance

- **Memory**: ~1KB per plan record
- **Startup**: ~1 second per 100K plans
- **Query time**: <10ms for typical queries
- **Large files**: Streaming parser for 20GB+ TOC files

## Limitations

1. **Not all plans are mappable**: Self-funded employer plans use EINs which aren't publicly linked to plan names
2. **Name variations**: Plan marketing names vary between sources
3. **State-based Exchanges**: NY uses its own exchange; some data may be in different formats
4. **Accuracy**: Fuzzy matching may return false positives; always verify results

## Best Practices

1. **Always provide state** - Most accurate filter
2. **Collect multiple inputs from users** - More data = better matching
3. **Use PUF data for marketplace plans** - Most reliable source
4. **Maintain issuer aliases** - Improves matching accuracy
5. **Set appropriate thresholds** - Higher min_score = fewer false positives

## License

MIT License - See LICENSE file
