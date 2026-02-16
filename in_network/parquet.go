package main

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

const flushInterval = 100_000

// RateParquetWriter writes rate rows to a Parquet file.
type RateParquetWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[RateRow]
	count  int
}

// NewRateParquetWriter creates a new Parquet writer for rate rows.
func NewRateParquetWriter(path string) (*RateParquetWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create rate parquet: %w", err)
	}
	writer := parquet.NewGenericWriter[RateRow](file,
		parquet.Compression(&parquet.Snappy),
	)
	return &RateParquetWriter{file: file, writer: writer}, nil
}

// Write writes a single rate row.
func (w *RateParquetWriter) Write(row RateRow) error {
	if _, err := w.writer.Write([]RateRow{row}); err != nil {
		return fmt.Errorf("write rate row: %w", err)
	}
	w.count++
	if w.count%flushInterval == 0 {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush rates: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the writer.
func (w *RateParquetWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close rate writer: %w", err)
	}
	return w.file.Close()
}

// Count returns the number of rows written.
func (w *RateParquetWriter) Count() int { return w.count }

// ProviderParquetWriter writes provider rows to a Parquet file.
type ProviderParquetWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[ProviderRow]
	count  int
}

// NewProviderParquetWriter creates a new Parquet writer for provider rows.
func NewProviderParquetWriter(path string) (*ProviderParquetWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create provider parquet: %w", err)
	}
	writer := parquet.NewGenericWriter[ProviderRow](file,
		parquet.Compression(&parquet.Snappy),
	)
	return &ProviderParquetWriter{file: file, writer: writer}, nil
}

// Write writes a single provider row.
func (w *ProviderParquetWriter) Write(row ProviderRow) error {
	if _, err := w.writer.Write([]ProviderRow{row}); err != nil {
		return fmt.Errorf("write provider row: %w", err)
	}
	w.count++
	if w.count%flushInterval == 0 {
		if err := w.writer.Flush(); err != nil {
			return fmt.Errorf("flush providers: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the writer.
func (w *ProviderParquetWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close provider writer: %w", err)
	}
	return w.file.Close()
}

// Count returns the number of rows written.
func (w *ProviderParquetWriter) Count() int { return w.count }
