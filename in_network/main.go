package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	inputFile := flag.String("file", "", "Input in-network JSON file (required, supports .gz)")
	outputBase := flag.String("out", "", "Output base path (default: derived from input filename)")
	npiFile := flag.String("npi", "", "NPI allowlist JSON file (optional, filters to matching providers)")
	bufferSize := flag.Int("buffer", 64, "Read buffer size in MB")
	verbose := flag.Bool("v", false, "Verbose output with progress updates")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `in_network - Convert CMS in-network rate JSON files to Parquet

Produces two Parquet files:
  <base>_rates.parquet      One row per negotiated price (denormalized)
  <base>_providers.parquet  One row per (provider_group_id, NPI)

Users JOIN on provider_group_id to resolve provider details.

Usage:
  in_network -file <input.json> [-out <base>] [-v]

Options:
`)
		flag.PrintDefaults()
	}

	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -file is required")
		flag.Usage()
		os.Exit(1)
	}

	// Determine output base path
	base := *outputBase
	if base == "" {
		base = *inputFile
		for _, ext := range []string{".gz", ".json"} {
			base = strings.TrimSuffix(base, ext)
		}
	}
	ratesPath := base + "_rates.parquet"
	providersPath := base + "_providers.parquet"

	startTime := time.Now()
	log.Printf("Input:  %s", *inputFile)
	log.Printf("Output: %s, %s", filepath.Base(ratesPath), filepath.Base(providersPath))

	// Open input file
	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open input: %v", err)
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	log.Printf("File size: %.2f MB", float64(fileInfo.Size())/(1024*1024))

	// Set up buffered reader with optional gzip
	var reader io.Reader
	bufSize := *bufferSize * 1024 * 1024
	br := bufio.NewReaderSize(file, bufSize)

	if strings.HasSuffix(strings.ToLower(*inputFile), ".gz") {
		gz, err := gzip.NewReader(br)
		if err != nil {
			log.Fatalf("Failed to create gzip reader: %v", err)
		}
		defer gz.Close()
		reader = gz
		log.Printf("Detected gzipped input")
	} else {
		reader = br
	}

	// Create Parquet writers
	rateWriter, err := NewRateParquetWriter(ratesPath)
	if err != nil {
		log.Fatalf("Failed to create rate writer: %v", err)
	}

	providerWriter, err := NewProviderParquetWriter(providersPath)
	if err != nil {
		rateWriter.Close()
		log.Fatalf("Failed to create provider writer: %v", err)
	}

	// Load NPI filter if specified
	converter := NewStreamConverter(reader, *verbose)
	if *npiFile != "" {
		filter, err := LoadNPIFilter(*npiFile)
		if err != nil {
			rateWriter.Close()
			providerWriter.Close()
			log.Fatalf("Failed to load NPI filter: %v", err)
		}
		converter.SetNPIFilter(filter)
		log.Printf("NPI filter: %d NPIs loaded from %s", len(filter), *npiFile)
	}

	// Convert
	stats, err := converter.Convert(rateWriter, providerWriter)
	if err != nil {
		rateWriter.Close()
		providerWriter.Close()
		log.Fatalf("Convert error: %v", err)
	}

	// Close writers
	if err := rateWriter.Close(); err != nil {
		providerWriter.Close()
		log.Fatalf("Failed to close rate writer: %v", err)
	}
	if err := providerWriter.Close(); err != nil {
		log.Fatalf("Failed to close provider writer: %v", err)
	}

	elapsed := time.Since(startTime)
	log.Printf("Done in %v", elapsed.Round(time.Millisecond))
	log.Printf("  %d in-network items → %d rate rows (%s)",
		stats.InNetworkItems, stats.RateRows, filepath.Base(ratesPath))
	log.Printf("  %d provider rows (%s)",
		stats.ProviderRows, filepath.Base(providersPath))
}
