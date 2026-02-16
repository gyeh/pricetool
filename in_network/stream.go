package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
)

// ConvertStats tracks conversion statistics.
type ConvertStats struct {
	InNetworkItems int64
	RateRows       int64
	ProviderRows   int64
}

// StreamConverter reads in-network JSON and writes to Parquet files.
type StreamConverter struct {
	decoder         *json.Decoder
	meta            RootMetadata
	verbose         bool
	nextProviderID  int32 // auto-increment for embedded provider groups
	npiFilter       map[int64]bool
	matchedGroupIDs map[int32]bool
}

// NewStreamConverter creates a new streaming converter.
func NewStreamConverter(r io.Reader, verbose bool) *StreamConverter {
	return &StreamConverter{
		decoder:         json.NewDecoder(r),
		verbose:         verbose,
		matchedGroupIDs: make(map[int32]bool),
	}
}

// SetNPIFilter sets an NPI allowlist. Only providers with matching NPIs
// and rates referencing those providers will be included in the output.
func (c *StreamConverter) SetNPIFilter(filter map[int64]bool) {
	c.npiFilter = filter
}

// Convert streams the JSON input and writes to both Parquet writers.
func (c *StreamConverter) Convert(rateWriter *RateParquetWriter, providerWriter *ProviderParquetWriter) (*ConvertStats, error) {
	stats := &ConvertStats{}

	// Read opening {
	t, err := c.decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("read opening token: %w", err)
	}
	if d, ok := t.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected {, got %v", t)
	}

	for c.decoder.More() {
		t, err := c.decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("read field name: %w", err)
		}
		field, ok := t.(string)
		if !ok {
			return nil, fmt.Errorf("expected field name, got %T", t)
		}

		switch field {
		case "reporting_entity_name":
			if err := c.decoder.Decode(&c.meta.ReportingEntityName); err != nil {
				return nil, fmt.Errorf("decode reporting_entity_name: %w", err)
			}
		case "reporting_entity_type":
			if err := c.decoder.Decode(&c.meta.ReportingEntityType); err != nil {
				return nil, fmt.Errorf("decode reporting_entity_type: %w", err)
			}
		case "plan_name":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode plan_name: %w", err)
			}
			c.meta.PlanName = &s
		case "issuer_name":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode issuer_name: %w", err)
			}
			c.meta.IssuerName = &s
		case "plan_sponsor_name":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode plan_sponsor_name: %w", err)
			}
			c.meta.PlanSponsorName = &s
		case "plan_id_type":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode plan_id_type: %w", err)
			}
			c.meta.PlanIDType = &s
		case "plan_id":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode plan_id: %w", err)
			}
			c.meta.PlanID = &s
		case "plan_market_type":
			var s string
			if err := c.decoder.Decode(&s); err != nil {
				return nil, fmt.Errorf("decode plan_market_type: %w", err)
			}
			c.meta.PlanMarketType = &s
		case "last_updated_on":
			if err := c.decoder.Decode(&c.meta.LastUpdatedOn); err != nil {
				return nil, fmt.Errorf("decode last_updated_on: %w", err)
			}
		case "version":
			if err := c.decoder.Decode(&c.meta.Version); err != nil {
				return nil, fmt.Errorf("decode version: %w", err)
			}
		case "provider_references":
			if err := c.streamProviderReferences(providerWriter, stats); err != nil {
				return nil, err
			}
		case "in_network":
			if err := c.streamInNetwork(rateWriter, providerWriter, stats); err != nil {
				return nil, err
			}
		default:
			var skip json.RawMessage
			if err := c.decoder.Decode(&skip); err != nil {
				return nil, fmt.Errorf("skip field %s: %w", field, err)
			}
		}
	}

	// Read closing }
	if _, err := c.decoder.Token(); err != nil {
		return nil, fmt.Errorf("read closing token: %w", err)
	}

	return stats, nil
}

func (c *StreamConverter) streamProviderReferences(w *ProviderParquetWriter, stats *ConvertStats) error {
	return c.streamArray(func() error {
		var ref ProviderReference
		if err := c.decoder.Decode(&ref); err != nil {
			return fmt.Errorf("decode provider_reference: %w", err)
		}

		matched := false
		for _, pg := range ref.ProviderGroups {
			var bizName *string
			if pg.TIN.BusinessName != "" {
				s := pg.TIN.BusinessName
				bizName = &s
			}
			for _, npi := range pg.NPI {
				if c.npiFilter != nil && !c.npiFilter[npi] {
					continue
				}
				matched = true
				row := ProviderRow{
					ProviderGroupID: int32(ref.ProviderGroupID),
					NPI:             npi,
					TINType:         pg.TIN.Type,
					TINValue:        pg.TIN.Value,
					BusinessName:    bizName,
					NetworkNames:    ref.NetworkName,
				}
				if err := w.Write(row); err != nil {
					return err
				}
				stats.ProviderRows++
			}
		}
		if matched {
			c.matchedGroupIDs[int32(ref.ProviderGroupID)] = true
		}
		return nil
	})
}

func (c *StreamConverter) streamInNetwork(w *RateParquetWriter, pw *ProviderParquetWriter, stats *ConvertStats) error {
	return c.streamArray(func() error {
		var item InNetworkItem
		if err := c.decoder.Decode(&item); err != nil {
			return fmt.Errorf("decode in_network item: %w", err)
		}
		stats.InNetworkItems++

		if c.verbose && stats.InNetworkItems%10000 == 0 {
			log.Printf("  processed %d in-network items, %d rate rows",
				stats.InNetworkItems, stats.RateRows)
		}

		// Serialize bundled_codes and covered_services to JSON strings
		var bundledJSON, coveredJSON *string
		if len(item.BundledCodes) > 0 {
			b, err := json.Marshal(item.BundledCodes)
			if err != nil {
				return fmt.Errorf("marshal bundled_codes: %w", err)
			}
			s := string(b)
			bundledJSON = &s
		}
		if len(item.CoveredServices) > 0 {
			b, err := json.Marshal(item.CoveredServices)
			if err != nil {
				return fmt.Errorf("marshal covered_services: %w", err)
			}
			s := string(b)
			coveredJSON = &s
		}

		for _, nr := range item.NegotiatedRates {
			var ids []int32

			if len(nr.ProviderGroups) > 0 {
				// Embedded provider groups: assign auto-incremented IDs and write provider rows
				for _, pg := range nr.ProviderGroups {
					c.nextProviderID++
					pgID := c.nextProviderID

					var bizName *string
					if pg.TIN.BusinessName != "" {
						s := pg.TIN.BusinessName
						bizName = &s
					}
					groupMatched := false
					for _, npi := range pg.NPI {
						if c.npiFilter != nil && !c.npiFilter[npi] {
							continue
						}
						groupMatched = true
						row := ProviderRow{
							ProviderGroupID: pgID,
							NPI:             npi,
							TINType:         pg.TIN.Type,
							TINValue:        pg.TIN.Value,
							BusinessName:    bizName,
						}
						if err := pw.Write(row); err != nil {
							return err
						}
						stats.ProviderRows++
					}
					if groupMatched {
						ids = append(ids, pgID)
					}
				}
			} else {
				// Referenced provider groups: use IDs as-is
				for _, id := range nr.ProviderReferences {
					pgID := int32(id)
					if c.npiFilter != nil && !c.matchedGroupIDs[pgID] {
						continue
					}
					ids = append(ids, pgID)
				}
			}

			if c.npiFilter != nil && len(ids) == 0 {
				continue
			}

			for _, price := range nr.NegotiatedPrices {
				var addlInfo *string
				if price.AdditionalInformation != "" {
					s := price.AdditionalInformation
					addlInfo = &s
				}

				row := RateRow{
					ReportingEntityName:    c.meta.ReportingEntityName,
					ReportingEntityType:    c.meta.ReportingEntityType,
					PlanName:               c.meta.PlanName,
					IssuerName:             c.meta.IssuerName,
					PlanSponsorName:        c.meta.PlanSponsorName,
					PlanIDType:             c.meta.PlanIDType,
					PlanID:                 c.meta.PlanID,
					PlanMarketType:         c.meta.PlanMarketType,
					LastUpdatedOn:          c.meta.LastUpdatedOn,
					Version:                c.meta.Version,
					NegotiationArrangement: item.NegotiationArrangement,
					Name:                   item.Name,
					BillingCodeType:        item.BillingCodeType,
					BillingCodeTypeVersion: item.BillingCodeTypeVersion,
					BillingCode:            item.BillingCode,
					Description:            item.Description,
					NegotiatedRate:         price.NegotiatedRate,
					NegotiatedType:         price.NegotiatedType,
					BillingClass:           price.BillingClass,
					Setting:                price.Setting,
					ExpirationDate:         price.ExpirationDate,
					ServiceCode:            price.ServiceCode,
					BillingCodeModifier:    price.BillingCodeModifier,
					AdditionalInformation:  addlInfo,
					ProviderGroupIDs:       ids,
					BundledCodesJSON:       bundledJSON,
					CoveredServicesJSON:    coveredJSON,
				}
				if err := w.Write(row); err != nil {
					return err
				}
				stats.RateRows++
			}
		}
		return nil
	})
}

// streamArray reads a JSON array token by token, calling fn for each element.
func (c *StreamConverter) streamArray(fn func() error) error {
	t, err := c.decoder.Token()
	if err != nil {
		return fmt.Errorf("read array start: %w", err)
	}
	if d, ok := t.(json.Delim); !ok || d != '[' {
		return fmt.Errorf("expected [, got %v", t)
	}
	for c.decoder.More() {
		if err := fn(); err != nil {
			return err
		}
	}
	// Read closing ]
	_, err = c.decoder.Token()
	return err
}
