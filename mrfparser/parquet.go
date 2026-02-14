package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// NYSPlanParquet is the Parquet-compatible output format
// Uses repeated string field for in_network_urls to leverage Parquet's compression
type NYSPlanParquet struct {
	PlanName       string   `parquet:"plan_name"`
	PlanIDType     string   `parquet:"plan_id_type"`
	PlanID         string   `parquet:"plan_id"`
	PlanMarketType string   `parquet:"plan_market_type"`
	IssuerName     string   `parquet:"issuer_name"`
	Description    string   `parquet:"description"`
	InNetworkURLs  []string `parquet:"in_network_urls,list"` // Array of URLs
	URLCount       int32    `parquet:"url_count"`
}

const parquetFlushInterval = 100_000

// ParquetWriter handles writing plans to a Parquet file
type ParquetWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[NYSPlanParquet]
	count  int
}

// NewParquetWriter creates a new Parquet file writer
func NewParquetWriter(filename string) (*ParquetWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[NYSPlanParquet](file,
		parquet.Compression(&parquet.Snappy),
	)

	return &ParquetWriter{
		file:   file,
		writer: writer,
	}, nil
}

// Write writes a plan to the Parquet file
func (pw *ParquetWriter) Write(plan NYSPlanOutput) error {
	record := NYSPlanParquet{
		PlanName:       plan.PlanName,
		PlanIDType:     plan.PlanIDType,
		PlanID:         plan.PlanID,
		PlanMarketType: plan.PlanMarketType,
		IssuerName:     plan.IssuerName,
		Description:    plan.Description,
		InNetworkURLs:  plan.InNetworkURLs,
		URLCount:       int32(len(plan.InNetworkURLs)),
	}

	_, err := pw.writer.Write([]NYSPlanParquet{record})
	if err != nil {
		return fmt.Errorf("failed to write parquet record: %w", err)
	}

	pw.count++

	// Flush row group periodically to bound memory usage
	if pw.count%parquetFlushInterval == 0 {
		if err := pw.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush parquet row group: %w", err)
		}
	}

	return nil
}

// Close flushes and closes the Parquet writer
func (pw *ParquetWriter) Close() error {
	if err := pw.writer.Close(); err != nil {
		pw.file.Close()
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}
	return pw.file.Close()
}

// Count returns the number of records written
func (pw *ParquetWriter) Count() int {
	return pw.count
}

// NormalizedPlanParquet is the plan row in normalized output (no URLs)
type NormalizedPlanParquet struct {
	ReportingStructureID int64  `parquet:"reporting_structure_id"`
	PlanName             string `parquet:"plan_name"`
	PlanIDType           string `parquet:"plan_id_type"`
	PlanID               string `parquet:"plan_id"`
	PlanMarketType       string `parquet:"plan_market_type"`
	IssuerName           string `parquet:"issuer_name"`
	Description          string `parquet:"description"`
}

// NormalizedURLParquet is the URL row in normalized output
type NormalizedURLParquet struct {
	ReportingStructureID int64  `parquet:"reporting_structure_id"`
	URL                  string `parquet:"url"`
}

// NormalizedParquetWriter writes plans and URLs to two separate parquet files
type NormalizedParquetWriter struct {
	planFile   *os.File
	urlFile    *os.File
	planWriter *parquet.GenericWriter[NormalizedPlanParquet]
	urlWriter  *parquet.GenericWriter[NormalizedURLParquet]
	planCount  int
	urlCount   int
	// Track last structure ID for which URLs were written to deduplicate
	lastURLStructureID int64
}

// NewNormalizedParquetWriter creates writers for plans and URLs parquet files.
// urlPath is derived from planPath: "foo.parquet" -> "foo_urls.parquet"
func NewNormalizedParquetWriter(planPath string) (*NormalizedParquetWriter, error) {
	urlPath := strings.TrimSuffix(planPath, ".parquet") + "_urls.parquet"

	planFile, err := os.Create(planPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create plan parquet file: %w", err)
	}

	urlFile, err := os.Create(urlPath)
	if err != nil {
		planFile.Close()
		return nil, fmt.Errorf("failed to create url parquet file: %w", err)
	}

	planWriter := parquet.NewGenericWriter[NormalizedPlanParquet](planFile,
		parquet.Compression(&parquet.Snappy),
	)
	urlWriter := parquet.NewGenericWriter[NormalizedURLParquet](urlFile,
		parquet.Compression(&parquet.Snappy),
	)

	return &NormalizedParquetWriter{
		planFile:   planFile,
		urlFile:    urlFile,
		planWriter: planWriter,
		urlWriter:  urlWriter,
	}, nil
}

// Write writes a plan row and its URLs (deduplicated per structure) to the parquet files
func (nw *NormalizedParquetWriter) Write(plan NYSPlanOutput) error {
	// Write plan row
	record := NormalizedPlanParquet{
		ReportingStructureID: plan.StructureID,
		PlanName:             plan.PlanName,
		PlanIDType:           plan.PlanIDType,
		PlanID:               plan.PlanID,
		PlanMarketType:       plan.PlanMarketType,
		IssuerName:           plan.IssuerName,
		Description:          plan.Description,
	}
	if _, err := nw.planWriter.Write([]NormalizedPlanParquet{record}); err != nil {
		return fmt.Errorf("failed to write plan parquet record: %w", err)
	}
	nw.planCount++

	if nw.planCount%parquetFlushInterval == 0 {
		if err := nw.planWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush plan parquet: %w", err)
		}
	}

	// Write URL rows only once per structure
	if plan.StructureID != nw.lastURLStructureID {
		nw.lastURLStructureID = plan.StructureID
		for _, u := range plan.InNetworkURLs {
			urlRecord := NormalizedURLParquet{
				ReportingStructureID: plan.StructureID,
				URL:                  u,
			}
			if _, err := nw.urlWriter.Write([]NormalizedURLParquet{urlRecord}); err != nil {
				return fmt.Errorf("failed to write url parquet record: %w", err)
			}
			nw.urlCount++

			if nw.urlCount%parquetFlushInterval == 0 {
				if err := nw.urlWriter.Flush(); err != nil {
					return fmt.Errorf("failed to flush url parquet: %w", err)
				}
			}
		}
	}

	return nil
}

// Close flushes and closes both parquet writers
func (nw *NormalizedParquetWriter) Close() error {
	var errs []error
	if err := nw.planWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close plan writer: %w", err))
	}
	if err := nw.planFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close plan file: %w", err))
	}
	if err := nw.urlWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close url writer: %w", err))
	}
	if err := nw.urlFile.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close url file: %w", err))
	}
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// PlanCount returns the number of plan records written
func (nw *NormalizedParquetWriter) PlanCount() int {
	return nw.planCount
}

// URLCount returns the number of URL records written
func (nw *NormalizedParquetWriter) URLCount() int {
	return nw.urlCount
}

// URLPath returns the URL parquet file path
func (nw *NormalizedParquetWriter) URLPath() string {
	return nw.urlFile.Name()
}
