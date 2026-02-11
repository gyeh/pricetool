package main

import (
	"fmt"
	"os"

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
