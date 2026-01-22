package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"pricetool/db"
)

//go:embed sql/schema.sql
var schema string

const defaultBatchSize = 1000

func main() {
	// CLI flags
	inputFile := flag.String("file", "", "Path to the JSON or CSV file to parse")
	dbHost := flag.String("host", "localhost", "PostgreSQL host")
	dbPort := flag.Int("port", 5432, "PostgreSQL port")
	dbUser := flag.String("user", "postgres", "PostgreSQL user")
	dbPassword := flag.String("password", "", "PostgreSQL password")
	dbName := flag.String("dbname", "hospital_pricing", "PostgreSQL database name")
	initSchema := flag.Bool("init", false, "Initialize database schema")
	batchSize := flag.Int("batch", defaultBatchSize, "Batch size for commits (number of items per transaction)")

	flag.Parse()

	if *inputFile == "" && !*initSchema {
		fmt.Println("Usage: pricetool -file <json_or_csv_file> [options]")
		fmt.Println("\nOptions:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := context.Background()

	// Build connection string
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		*dbUser, *dbPassword, *dbHost, *dbPort, *dbName)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("Failed to parse connection string: %v", err)
	}

	// Configure pool for large imports
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to database successfully")

	if *initSchema {
		if err := initializeSchema(ctx, pool); err != nil {
			log.Fatalf("Failed to initialize schema: %v", err)
		}
		log.Println("Schema initialized successfully")
		if *inputFile == "" {
			return
		}
	}

	// Detect file type and process accordingly
	if strings.HasSuffix(strings.ToLower(*inputFile), ".csv") {
		if err := streamProcessCSV(ctx, pool, *inputFile, *batchSize); err != nil {
			log.Fatalf("Failed to process CSV file: %v", err)
		}
	} else {
		// Default to JSON processing
		if err := streamProcessJSON(ctx, pool, *inputFile, *batchSize); err != nil {
			log.Fatalf("Failed to process JSON file: %v", err)
		}
	}
	log.Println("Data import completed successfully")
}

func initializeSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schema)
	return err
}

// streamProcessCSV processes a large CSV file using streaming to minimize memory usage
func streamProcessCSV(ctx context.Context, pool *pgxpool.Pool, filePath string, batchSize int) error {
	reader, err := NewCSVStreamReader(filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Read CSV header
	header, err := reader.ReadHeader()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	log.Printf("Detected CSV format: %s", reader.Format())
	log.Printf("Hospital: %s, Version: %s", header.HospitalName, header.Version)

	// Insert hospital from CSV header
	hospitalID, err := insertCSVHospitalHeader(ctx, pool, header)
	if err != nil {
		return fmt.Errorf("failed to insert hospital: %w", err)
	}
	log.Printf("Inserted hospital '%s' with ID: %d", header.HospitalName, hospitalID)

	// Stream and process items
	var count int64
	var tx pgx.Tx
	var queries *db.Queries
	var batchCount int

	// Start first transaction
	tx, err = pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	queries = db.New(tx)

	startTime := time.Now()
	lastLogTime := startTime

	for {
		sci, err := reader.NextItem()
		if err == io.EOF {
			break
		}
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to read item at row %d: %w", reader.RowNum(), err)
		}

		if err := insertStandardChargeInfo(ctx, queries, hospitalID, sci); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to insert item at row %d: %w", reader.RowNum(), err)
		}

		count++
		batchCount++

		// Commit batch and start new transaction
		if batchCount >= batchSize {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit batch at row %d: %w", reader.RowNum(), err)
			}

			// Log progress periodically
			now := time.Now()
			if now.Sub(lastLogTime) >= 5*time.Second {
				elapsed := now.Sub(startTime)
				rate := float64(count) / elapsed.Seconds()
				log.Printf("  Progress: %d items processed (%.1f items/sec), row %d",
					count, rate, reader.RowNum())
				lastLogTime = now
			}

			tx, err = pool.Begin(ctx)
			if err != nil {
				return fmt.Errorf("failed to begin new transaction: %w", err)
			}
			queries = db.New(tx)
			batchCount = 0
		}
	}

	// Commit final batch
	if batchCount > 0 {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit final batch: %w", err)
		}
	} else {
		tx.Rollback(ctx) // Nothing to commit
	}

	elapsed := time.Since(startTime)
	rate := float64(count) / elapsed.Seconds()
	log.Printf("CSV import complete: %d items in %v (%.1f items/sec)", count, elapsed, rate)
	return nil
}

// insertCSVHospitalHeader inserts hospital data from CSV header
func insertCSVHospitalHeader(ctx context.Context, pool *pgxpool.Pool, header *CSVHeader) (int32, error) {
	queries := db.New(pool)

	// Parse date
	date, err := time.Parse("2006-01-02", header.LastUpdatedOn)
	if err != nil {
		// Try alternate formats
		date, err = time.Parse("01/02/2006", header.LastUpdatedOn)
		if err != nil {
			date = time.Now() // Fallback to current date
		}
	}

	// Get first license info
	var licenseNum, licenseState string
	for state, num := range header.LicenseNumbers {
		licenseState = state
		licenseNum = num
		break
	}

	params := db.InsertHospitalParams{
		Name:          header.HospitalName,
		Addresses:     header.HospitalAddresses,
		LocationNames: header.HospitalLocations,
		Npis:          nil,
		LicenseNumber: toTextFromString(licenseNum),
		LicenseState:  toTextFromString(licenseState),
		Version:       header.Version,
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{Valid: false},
	}

	return queries.InsertHospital(ctx, params)
}

// streamProcessJSON processes a large JSON file using streaming to minimize memory usage
func streamProcessJSON(ctx context.Context, pool *pgxpool.Pool, filePath string, batchSize int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Use buffered reader for better I/O performance
	reader := bufio.NewReaderSize(file, 64*1024) // 64KB buffer

	// Skip UTF-8 BOM if present (0xEF 0xBB 0xBF)
	bom, err := reader.Peek(3)
	if err == nil && len(bom) >= 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF {
		reader.Discard(3)
	}

	decoder := json.NewDecoder(reader)

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening token: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected opening brace, got %v", token)
	}

	// Collect header fields and track array positions
	header := &HospitalHeader{}
	var hospitalID int32
	var chargeCount, modifierCount int64

	// Process fields one by one
	for decoder.More() {
		// Read field name
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read field name: %w", err)
		}

		fieldName, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected field name string, got %v", token)
		}

		switch fieldName {
		case "hospital_name":
			if err := decoder.Decode(&header.HospitalName); err != nil {
				return fmt.Errorf("failed to decode hospital_name: %w", err)
			}

		case "hospital_address":
			if err := decoder.Decode(&header.HospitalAddress); err != nil {
				return fmt.Errorf("failed to decode hospital_address: %w", err)
			}

		case "last_updated_on":
			if err := decoder.Decode(&header.LastUpdatedOn); err != nil {
				return fmt.Errorf("failed to decode last_updated_on: %w", err)
			}

		case "attestation":
			if err := decoder.Decode(&header.Attestation); err != nil {
				return fmt.Errorf("failed to decode attestation: %w", err)
			}

		case "license_information":
			if err := decoder.Decode(&header.LicenseInformation); err != nil {
				return fmt.Errorf("failed to decode license_information: %w", err)
			}

		case "version":
			if err := decoder.Decode(&header.Version); err != nil {
				return fmt.Errorf("failed to decode version: %w", err)
			}

		case "location_name":
			if err := decoder.Decode(&header.LocationName); err != nil {
				return fmt.Errorf("failed to decode location_name: %w", err)
			}

		case "type_2_npi":
			if err := decoder.Decode(&header.Type2NPI); err != nil {
				return fmt.Errorf("failed to decode type_2_npi: %w", err)
			}

		case "standard_charge_information":
			// Insert hospital first if not done yet
			if hospitalID == 0 {
				var err error
				hospitalID, err = insertHospitalHeader(ctx, pool, header)
				if err != nil {
					return fmt.Errorf("failed to insert hospital: %w", err)
				}
				log.Printf("Inserted hospital '%s' with ID: %d", header.HospitalName, hospitalID)
			}

			// Stream through the array
			chargeCount, err = streamStandardCharges(ctx, pool, decoder, hospitalID, batchSize)
			if err != nil {
				return fmt.Errorf("failed to stream standard charges: %w", err)
			}
			log.Printf("Processed %d standard charge items", chargeCount)

		case "modifier_information":
			// Insert hospital first if not done yet
			if hospitalID == 0 {
				var err error
				hospitalID, err = insertHospitalHeader(ctx, pool, header)
				if err != nil {
					return fmt.Errorf("failed to insert hospital: %w", err)
				}
				log.Printf("Inserted hospital '%s' with ID: %d", header.HospitalName, hospitalID)
			}

			// Stream through the array
			modifierCount, err = streamModifiers(ctx, pool, decoder, hospitalID, batchSize)
			if err != nil {
				return fmt.Errorf("failed to stream modifiers: %w", err)
			}
			log.Printf("Processed %d modifiers", modifierCount)

		default:
			// Skip unknown fields
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return fmt.Errorf("failed to skip field %s: %w", fieldName, err)
			}
		}
	}

	// Insert hospital if we never hit the arrays
	if hospitalID == 0 {
		hospitalID, err = insertHospitalHeader(ctx, pool, header)
		if err != nil {
			return fmt.Errorf("failed to insert hospital: %w", err)
		}
		log.Printf("Inserted hospital '%s' with ID: %d", header.HospitalName, hospitalID)
	}

	log.Printf("Import complete: %d standard charges, %d modifiers", chargeCount, modifierCount)
	return nil
}

// streamStandardCharges streams through the standard_charge_information array
func streamStandardCharges(ctx context.Context, pool *pgxpool.Pool, decoder *json.Decoder, hospitalID int32, batchSize int) (int64, error) {
	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("failed to read array start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return 0, fmt.Errorf("expected array start, got %v", token)
	}

	var count int64
	var tx pgx.Tx
	var queries *db.Queries
	var batchCount int

	// Start first transaction
	tx, err = pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	queries = db.New(tx)

	// Process each element
	for decoder.More() {
		var sci StandardChargeInformation
		if err := decoder.Decode(&sci); err != nil {
			tx.Rollback(ctx)
			return count, fmt.Errorf("failed to decode item %d: %w", count, err)
		}

		if err := insertStandardChargeInfo(ctx, queries, hospitalID, &sci); err != nil {
			tx.Rollback(ctx)
			return count, fmt.Errorf("failed to insert item %d: %w", count, err)
		}

		count++
		batchCount++

		// Commit batch and start new transaction
		if batchCount >= batchSize {
			if err := tx.Commit(ctx); err != nil {
				return count, fmt.Errorf("failed to commit batch at item %d: %w", count, err)
			}
			log.Printf("  Committed batch: %d items processed", count)

			tx, err = pool.Begin(ctx)
			if err != nil {
				return count, fmt.Errorf("failed to begin new transaction: %w", err)
			}
			queries = db.New(tx)
			batchCount = 0
		}
	}

	// Commit final batch
	if batchCount > 0 {
		if err := tx.Commit(ctx); err != nil {
			return count, fmt.Errorf("failed to commit final batch: %w", err)
		}
	} else {
		tx.Rollback(ctx) // Nothing to commit
	}

	// Read closing bracket
	token, err = decoder.Token()
	if err != nil && err != io.EOF {
		return count, fmt.Errorf("failed to read array end: %w", err)
	}

	return count, nil
}

// streamModifiers streams through the modifier_information array
func streamModifiers(ctx context.Context, pool *pgxpool.Pool, decoder *json.Decoder, hospitalID int32, batchSize int) (int64, error) {
	// Read opening bracket
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("failed to read array start: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return 0, fmt.Errorf("expected array start, got %v", token)
	}

	var count int64
	var tx pgx.Tx
	var queries *db.Queries
	var batchCount int

	// Start first transaction
	tx, err = pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	queries = db.New(tx)

	// Process each element
	for decoder.More() {
		var mod ModifierInformation
		if err := decoder.Decode(&mod); err != nil {
			tx.Rollback(ctx)
			return count, fmt.Errorf("failed to decode modifier %d: %w", count, err)
		}

		if err := insertModifier(ctx, queries, hospitalID, &mod); err != nil {
			tx.Rollback(ctx)
			return count, fmt.Errorf("failed to insert modifier %d: %w", count, err)
		}

		count++
		batchCount++

		// Commit batch and start new transaction
		if batchCount >= batchSize {
			if err := tx.Commit(ctx); err != nil {
				return count, fmt.Errorf("failed to commit batch at modifier %d: %w", count, err)
			}
			log.Printf("  Committed modifier batch: %d items processed", count)

			tx, err = pool.Begin(ctx)
			if err != nil {
				return count, fmt.Errorf("failed to begin new transaction: %w", err)
			}
			queries = db.New(tx)
			batchCount = 0
		}
	}

	// Commit final batch
	if batchCount > 0 {
		if err := tx.Commit(ctx); err != nil {
			return count, fmt.Errorf("failed to commit final batch: %w", err)
		}
	} else {
		tx.Rollback(ctx) // Nothing to commit
	}

	// Read closing bracket
	token, err = decoder.Token()
	if err != nil && err != io.EOF {
		return count, fmt.Errorf("failed to read array end: %w", err)
	}

	return count, nil
}

func insertHospitalHeader(ctx context.Context, pool *pgxpool.Pool, header *HospitalHeader) (int32, error) {
	queries := db.New(pool)

	// Parse date
	date, err := time.Parse("2006-01-02", header.LastUpdatedOn)
	if err != nil {
		return 0, fmt.Errorf("failed to parse date: %w", err)
	}

	// Handle V2 vs V3 field differences
	locationNames := header.LocationName
	if len(locationNames) == 0 {
		locationNames = header.HospitalLocation // V2 uses hospital_location
	}

	// Get attester name from attestation (V3) or affirmation (V2)
	attesterName := header.Attestation.AttesterName
	if attesterName == "" {
		attesterName = header.Attestation.Attestation // V3 attestation text
	}
	if attesterName == "" {
		attesterName = header.Affirmation.Affirmation // V2 affirmation text
	}

	params := db.InsertHospitalParams{
		Name:          header.HospitalName,
		Addresses:     header.HospitalAddress,
		LocationNames: locationNames,
		Npis:          header.Type2NPI,
		LicenseNumber: toText(header.LicenseInformation.LicenseNumber),
		LicenseState:  toTextFromString(header.LicenseInformation.State),
		Version:       header.Version,
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  toTextFromString(attesterName),
	}

	return queries.InsertHospital(ctx, params)
}

func insertStandardChargeInfo(ctx context.Context, queries *db.Queries, hospitalID int32, sci *StandardChargeInformation) error {
	// Insert the item
	var drugUnit pgtype.Numeric
	var drugUnitType pgtype.Text

	if sci.DrugInformation != nil {
		drugUnit = toNumeric(sci.DrugInformation.Unit.Value)
		drugUnitType = pgtype.Text{String: sci.DrugInformation.Type, Valid: true}
	}

	itemID, err := queries.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
		HospitalID:   hospitalID,
		Description:  sci.Description,
		DrugUnit:     drugUnit,
		DrugUnitType: drugUnitType,
	})
	if err != nil {
		return fmt.Errorf("failed to insert item: %w", err)
	}

	// Insert codes and link them
	for _, codeInfo := range sci.CodeInformation {
		codeID, err := queries.UpsertCode(ctx, db.UpsertCodeParams{
			Code:     codeInfo.Code,
			CodeType: codeInfo.Type,
		})
		if err != nil {
			return fmt.Errorf("failed to upsert code: %w", err)
		}

		err = queries.InsertItemCode(ctx, db.InsertItemCodeParams{
			ItemID: itemID,
			CodeID: codeID,
		})
		if err != nil {
			return fmt.Errorf("failed to link item to code: %w", err)
		}
	}

	// Insert standard charges
	for _, sc := range sci.StandardCharges {
		// Handle V2 gross_charges (string) vs V3 gross_charge (number)
		grossCharge := sc.GrossCharge
		if grossCharge == nil && sc.GrossCharges != nil {
			grossCharge = sc.GrossCharges.Value
		}

		chargeID, err := queries.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
			ItemID:          itemID,
			Setting:         sc.Setting,
			GrossCharge:     toNumeric(grossCharge),
			DiscountedCash:  toNumeric(sc.DiscountedCash),
			Minimum:         toNumeric(sc.Minimum),
			Maximum:         toNumeric(sc.Maximum),
			ModifierCodes:   sc.ModifierCode,
			AdditionalNotes: toText(sc.AdditionalGenericNotes),
		})
		if err != nil {
			return fmt.Errorf("failed to insert standard charge: %w", err)
		}

		// Insert payer information
		for _, pi := range sc.PayersInformation {
			err := queries.InsertPayerCharge(ctx, db.InsertPayerChargeParams{
				StandardChargeID:         chargeID,
				PayerName:                pi.PayerName,
				PlanName:                 pi.PlanName,
				Methodology:              pi.Methodology,
				StandardChargeDollar:     toNumeric(pi.StandardChargeDollar),
				StandardChargePercentage: toNumeric(pi.StandardChargePercentage),
				StandardChargeAlgorithm:  toText(pi.StandardChargeAlgorithm),
				EstimatedAmount:          toNumeric(pi.EstimatedAmount),
				MedianAmount:             toNumeric(pi.MedianAmount),
				Percentile10th:           toNumeric(pi.Percentile10th),
				Percentile90th:           toNumeric(pi.Percentile90th),
				Count:                    toText(pi.Count),
				AdditionalNotes:          toText(pi.AdditionalPayerNotes),
			})
			if err != nil {
				return fmt.Errorf("failed to insert payer charge: %w", err)
			}
		}
	}

	return nil
}

func insertModifier(ctx context.Context, queries *db.Queries, hospitalID int32, mod *ModifierInformation) error {
	modifierID, err := queries.InsertModifier(ctx, db.InsertModifierParams{
		HospitalID:  hospitalID,
		Code:        mod.Code,
		Description: mod.Description,
		Setting:     toText(mod.Setting),
	})
	if err != nil {
		return err
	}

	for _, mpi := range mod.ModifierPayerInformation {
		err := queries.InsertModifierPayerInfo(ctx, db.InsertModifierPayerInfoParams{
			ModifierID:  modifierID,
			PayerName:   mpi.PayerName,
			PlanName:    mpi.PlanName,
			Description: mpi.Description,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Helper functions for pgtype conversion

func toText(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func toTextFromString(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: s, Valid: true}
}

func toNumeric(f *float64) pgtype.Numeric {
	if f == nil {
		return pgtype.Numeric{Valid: false}
	}
	// Convert float64 to pgtype.Numeric
	// Use big.Float for precision
	bf := big.NewFloat(*f)
	text := bf.Text('f', -1)

	var num pgtype.Numeric
	num.Scan(text)
	return num
}
