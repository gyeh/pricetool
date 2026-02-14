package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
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
	// CLI flags
	inputFile := flag.String("file", "", "Input TOC JSON file (required, supports .json and .json.gz)")
	outputFile := flag.String("out", "", "Output file for extracted plans (default based on format)")
	outputFormat := flag.String("format", "json", "Output format: json or parquet")
	stateCode := flag.String("state", "", "2-letter state code to filter by HIOS ID (e.g., NY, CA, TX)")
	marketType := flag.String("market", "", "Filter by market type: 'individual' (marketplace/ACA), 'group', or '' for both")
	noHIOS := flag.Bool("no-hios", false, "Disable HIOS state code matching (use keywords only)")
	noKeywords := flag.Bool("no-keywords", false, "Disable keyword matching (use HIOS only)")
	keywords := flag.String("keywords", "", "Additional comma-separated keywords to match")
	verbose := flag.Bool("v", false, "Verbose output with progress updates")
	dryRun := flag.Bool("dry-run", false, "Parse file but don't write output (useful for testing)")
	bufferSize := flag.Int("buffer", 64, "Read buffer size in MB (default 64MB)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `mrfparser - Extract state-specific plans from large MRF Table of Contents files

This tool streams through multi-gigabyte TOC JSON files to extract
health insurance plans by state and their in-network file URLs.

Usage:
  mrfparser -file <input.json> [-out <output.json>] [options]

Options:
`)
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, `
Examples:
  # Extract NYS individual marketplace plans (most accurate)
  mrfparser -file toc.json -state NY -market individual

  # Extract all NYS plans (individual + group)
  mrfparser -file toc.json -state NY

  # Extract California plans
  mrfparser -file toc.json -state CA -out ca_plans.json

  # Output to Parquet format
  mrfparser -file toc.json -format parquet -out nys_plans.parquet

  # Use only HIOS matching (no keyword fallback)
  mrfparser -file toc.json -state NY -no-keywords

  # Add custom keywords for matching
  mrfparser -file toc.json -keywords "upstate,westchester"

  # Dry run to check file without writing output
  mrfparser -file toc.json -dry-run -v

HIOS ID Matching:
  The primary matching method uses the HIOS ID structure:
  Format: [5-digit issuer][2-char STATE][3-digit product][4-digit component]
  Example: 12345NY0010001 - "NY" at positions 6-7 indicates New York

  This is the most accurate method for identifying state-specific
  marketplace (ACA/QHP) plans.

Output Formats:
  JSON: Contains metadata and array of plans with in_network_urls as array
  Parquet: Columnar format with in_network_urls as repeated/list type
           Uses Snappy compression, includes url_count column

Output Fields:
  - plan_name: Name of the health plan
  - plan_id_type: "ein" or "hios"
  - plan_id: The plan identifier
  - plan_market_type: "group" or "individual"
  - issuer_name: Name of the plan issuer
  - description: Human-readable description
  - in_network_urls: URLs to in-network rate files (array/list type)
  - url_count: Number of in-network URLs (Parquet only)
`)
	}

	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -file is required")
		flag.Usage()
		os.Exit(1)
	}

	// Validate and set output format
	*outputFormat = strings.ToLower(*outputFormat)
	if *outputFormat != "json" && *outputFormat != "parquet" {
		fmt.Fprintln(os.Stderr, "Error: -format must be 'json' or 'parquet'")
		os.Exit(1)
	}

	// Validate state code (if provided)
	*stateCode = strings.ToUpper(*stateCode)
	if *stateCode != "" && len(*stateCode) != 2 {
		fmt.Fprintln(os.Stderr, "Error: -state must be a 2-letter state code (e.g., NY, CA, TX)")
		os.Exit(1)
	}

	// Validate market type
	*marketType = strings.ToLower(*marketType)
	if *marketType != "" && *marketType != "individual" && *marketType != "group" {
		fmt.Fprintln(os.Stderr, "Error: -market must be 'individual', 'group', or '' (empty for both)")
		os.Exit(1)
	}

	// Set default output file based on format and state
	if *outputFile == "" {
		base := "plans"
		if *stateCode != "" {
			base = strings.ToLower(*stateCode) + "_plans"
		}
		if *outputFormat == "parquet" {
			*outputFile = base + ".parquet"
		} else {
			*outputFile = base + ".json"
		}
	}

	// Configure the filter
	// Enable HIOS/keyword matching only when a state is specified
	useHIOS := *stateCode != "" && !*noHIOS
	useKW := *stateCode != "" && !*noKeywords
	CurrentFilter = FilterConfig{
		MarketType:       *marketType,
		UseHIOSStateCode: useHIOS,
		StateCode:        *stateCode,
		UseKeywords:      useKW,
	}

	// Add custom keywords if provided
	if *keywords != "" {
		customKeywords := strings.Split(*keywords, ",")
		for _, kw := range customKeywords {
			kw = strings.TrimSpace(strings.ToLower(kw))
			if kw != "" {
				NYSKeywords = append(NYSKeywords, kw)
			}
		}
	}

	startTime := time.Now()
	log.Printf("Starting MRF TOC parser...")
	log.Printf("Input file: %s", *inputFile)
	if !*dryRun {
		log.Printf("Output file: %s (format: %s)", *outputFile, *outputFormat)
	}

	// Log filter configuration
	marketDesc := "all"
	if CurrentFilter.MarketType != "" {
		marketDesc = CurrentFilter.MarketType
	}
	stateDesc := "none"
	if CurrentFilter.StateCode != "" {
		stateDesc = CurrentFilter.StateCode
	}
	log.Printf("Filter: state=%s, market=%s, hios=%v, keywords=%v",
		stateDesc, marketDesc,
		CurrentFilter.UseHIOSStateCode, CurrentFilter.UseKeywords)

	// Open input file
	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer file.Close()

	// Get file size for progress reporting
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatalf("Failed to stat input file: %v", err)
	}
	fileSize := fileInfo.Size()
	log.Printf("File size: %.2f GB", float64(fileSize)/(1024*1024*1024))

	// Set up reader with buffering
	var reader io.Reader
	bufSize := *bufferSize * 1024 * 1024
	bufferedReader := bufio.NewReaderSize(file, bufSize)

	// Handle gzipped files
	if strings.HasSuffix(strings.ToLower(*inputFile), ".gz") {
		gzReader, err := gzip.NewReader(bufferedReader)
		if err != nil {
			log.Fatalf("Failed to create gzip reader: %v", err)
		}
		defer gzReader.Close()
		reader = gzReader
		log.Printf("Detected gzipped input file")
	} else {
		reader = bufferedReader
	}

	// Create streaming parser
	parser := NewStreamParser(reader)

	// For Parquet, we can stream directly to file
	// For JSON, we need to collect all plans first (for the wrapper object)
	var matchedPlans []NYSPlanOutput
	var parquetWriter *NormalizedParquetWriter

	if !*dryRun && *outputFormat == "parquet" {
		// Ensure output directory exists
		outDir := filepath.Dir(*outputFile)
		if outDir != "" && outDir != "." {
			if err := os.MkdirAll(outDir, 0755); err != nil {
				log.Fatalf("Failed to create output directory: %v", err)
			}
		}

		parquetWriter, err = NewNormalizedParquetWriter(*outputFile)
		if err != nil {
			log.Fatalf("Failed to create parquet writer: %v", err)
		}
		defer parquetWriter.Close()
	}

	// Progress callback
	lastProgress := time.Now()
	onProgress := func(stats ParserStats) {
		if *verbose && time.Since(lastProgress) > 5*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(stats.TotalStructures) / elapsed.Seconds()
			log.Printf("Progress: %d structures processed (%.0f/sec), %d NYS plans found",
				stats.TotalStructures, rate, stats.MatchedPlans)
			lastProgress = time.Now()
		}
	}

	// Plan callback
	onPlan := func(plan NYSPlanOutput) {
		if parquetWriter != nil {
			// Stream directly to Parquet
			if err := parquetWriter.Write(plan); err != nil {
				log.Fatalf("Failed to write to parquet: %v", err)
			}
		} else if !*dryRun {
			// Collect for JSON output
			matchedPlans = append(matchedPlans, plan)
		}

		if *verbose {
			count := len(matchedPlans)
			if parquetWriter != nil {
				count = parquetWriter.PlanCount()
			}
			if count%100 == 0 {
				log.Printf("Found %d NYS plans so far...", count)
			}
		}
	}

	// Parse the file
	log.Printf("Parsing TOC file (streaming mode)...")
	if err := parser.Parse(onPlan, onProgress); err != nil {
		log.Fatalf("Parse error: %v", err)
	}

	stats := parser.GetStats()
	elapsed := time.Since(startTime)

	// Print summary
	log.Printf("Parsing complete!")
	log.Printf("  Total reporting structures: %d", stats.TotalStructures)
	log.Printf("  Total plans scanned: %d", stats.TotalPlans)
	log.Printf("  NYS plans found: %d", stats.MatchedPlans)
	log.Printf("  Elapsed time: %v", elapsed.Round(time.Second))
	if elapsed.Seconds() > 0 {
		log.Printf("  Processing rate: %.0f structures/sec", float64(stats.TotalStructures)/elapsed.Seconds())
	}

	if *dryRun {
		log.Printf("Dry run complete - no output file written")
		return
	}

	// Finalize output
	if parquetWriter != nil {
		// Parquet was written streaming, just close it
		if err := parquetWriter.Close(); err != nil {
			log.Fatalf("Failed to finalize parquet file: %v", err)
		}
		log.Printf("Successfully wrote %d plans to %s (Parquet)", parquetWriter.PlanCount(), *outputFile)
		log.Printf("Successfully wrote %d URLs to %s (Parquet)", parquetWriter.URLCount(), parquetWriter.URLPath())
	} else {
		// Write JSON output
		log.Printf("Writing output to %s...", *outputFile)

		// Ensure output directory exists
		outDir := filepath.Dir(*outputFile)
		if outDir != "" && outDir != "." {
			if err := os.MkdirAll(outDir, 0755); err != nil {
				log.Fatalf("Failed to create output directory: %v", err)
			}
		}

		metadata := parser.GetMetadata()
		output := OutputFile{
			ReportingEntityName: metadata.ReportingEntityName,
			ReportingEntityType: metadata.ReportingEntityType,
			LastUpdatedOn:       metadata.LastUpdatedOn,
			ExtractedAt:         time.Now().UTC().Format(time.RFC3339),
			TotalPlansExtracted: len(matchedPlans),
			Plans:               matchedPlans,
		}

		outFile, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("Failed to create output file: %v", err)
		}
		defer outFile.Close()

		encoder := json.NewEncoder(outFile)
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(output); err != nil {
			log.Fatalf("Failed to write output: %v", err)
		}

		log.Printf("Successfully wrote %d NYS plans to %s (JSON)", len(matchedPlans), *outputFile)
	}
}
