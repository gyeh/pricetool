package main

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/parquet-go/parquet-go"
)

func loadParquetToPg(ctx context.Context, parquetPath, connStr string, batchSize int) error {
	start := time.Now()

	f, err := os.Open(parquetPath)
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat parquet: %w", err)
	}

	reader := parquet.NewGenericReader[HospitalChargeRow](f)
	defer reader.Close()

	totalRows := reader.NumRows()
	fmt.Printf("Input:  %s\n", parquetPath)
	fmt.Printf("Size:   %.1f MB\n", float64(fi.Size())/1024/1024)
	fmt.Printf("Rows:   %d\n", totalRows)
	fmt.Println()

	// Connect to PostgreSQL
	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return fmt.Errorf("parse connection: %w", err)
	}
	poolConfig.MaxConns = 4

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	fmt.Println("Connected to PostgreSQL")

	// Code cache: "CODE\tTYPE" → code_id
	codeCache := make(map[string]int32)
	// Plan cache: plan_name → plan_id
	planCache := make(map[string]int32)

	const readBatch = 8192
	buf := make([]HospitalChargeRow, readBatch)

	var (
		hospitalID  int32
		tx          pgx.Tx
		itemCount   int
		chargeCount int
		payerCount  int64
		rowsRead    int64
		lastLog     = time.Now()

		// Current item accumulator
		curItemKey  string
		curItemRows []HospitalChargeRow
		txItemCount int

		// Accumulated payer_charges for bulk COPY within a transaction
		pendingPayers [][]interface{}
	)

	payerCopyCols := []string{
		"standard_charge_id", "payer_name", "plan_id", "methodology",
		"standard_charge_dollar", "standard_charge_percentage",
		"standard_charge_algorithm", "estimated_amount", "median_amount",
		"percentile_10th", "percentile_90th", "count", "additional_notes",
	}

	// flushPayers bulk-inserts accumulated payer_charges via COPY.
	flushPayers := func() error {
		if len(pendingPayers) == 0 {
			return nil
		}
		copied, err := tx.CopyFrom(ctx,
			pgx.Identifier{"payer_charges"},
			payerCopyCols,
			pgx.CopyFromRows(pendingPayers),
		)
		if err != nil {
			return fmt.Errorf("copy payer_charges: %w", err)
		}
		payerCount += copied
		pendingPayers = pendingPayers[:0]
		return nil
	}

	// flushItem processes accumulated rows for one item.
	flushItem := func() error {
		if len(curItemRows) == 0 {
			return nil
		}

		first := curItemRows[0]

		// Insert standard_charge_item
		var drugUnit pgtype.Numeric
		var drugUnitType pgtype.Text
		if first.DrugUnitOfMeasurement != nil {
			drugUnit = floatToNumeric(first.DrugUnitOfMeasurement)
		}
		if first.DrugTypeOfMeasurement != nil {
			drugUnitType = pgtype.Text{String: *first.DrugTypeOfMeasurement, Valid: true}
		}

		var itemID int32
		err := tx.QueryRow(ctx,
			`INSERT INTO standard_charge_items (hospital_id, description, drug_unit, drug_unit_type)
			 VALUES ($1, $2, $3, $4) RETURNING id`,
			hospitalID, sanitizeUTF8(first.Description), drugUnit, drugUnitType,
		).Scan(&itemID)
		if err != nil {
			return fmt.Errorf("insert item: %w", err)
		}
		itemCount++

		// Upsert codes and link to item
		codes := collectCodes(&first)
		for _, cp := range codes {
			cacheKey := cp[0] + "\t" + cp[1]
			codeID, ok := codeCache[cacheKey]
			if !ok {
				err := tx.QueryRow(ctx,
					`INSERT INTO codes (code, code_type) VALUES ($1, $2)
					 ON CONFLICT (code, code_type) DO UPDATE SET code = EXCLUDED.code
					 RETURNING id`,
					cp[0], cp[1],
				).Scan(&codeID)
				if err != nil {
					return fmt.Errorf("upsert code %s/%s: %w", cp[0], cp[1], err)
				}
				codeCache[cacheKey] = codeID
			}

			_, err := tx.Exec(ctx,
				`INSERT INTO item_codes (item_id, code_id) VALUES ($1, $2)
				 ON CONFLICT (item_id, code_id) DO NOTHING`,
				itemID, codeID,
			)
			if err != nil {
				return fmt.Errorf("link item-code: %w", err)
			}
		}

		// Group rows by charge key (setting + gross/discounted/min/max/modifiers/notes)
		type chargeGroup struct {
			rows []HospitalChargeRow
		}
		var groups []chargeGroup
		var curCK string

		for i := range curItemRows {
			ck := chargeKeyOf(&curItemRows[i])
			if ck != curCK || len(groups) == 0 {
				groups = append(groups, chargeGroup{})
				curCK = ck
			}
			groups[len(groups)-1].rows = append(groups[len(groups)-1].rows, curItemRows[i])
		}

		// Insert each charge group
		for _, g := range groups {
			r := g.rows[0]

			var modifierCodes []string
			if r.Modifiers != nil && *r.Modifiers != "" {
				modifierCodes = strings.Split(*r.Modifiers, "|")
			}

			var chargeID int32
			err := tx.QueryRow(ctx,
				`INSERT INTO standard_charges
				 (item_id, setting, gross_charge, discounted_cash, minimum, maximum, modifier_codes, additional_notes)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
				itemID,
				r.Setting,
				floatToNumeric(r.GrossCharge),
				floatToNumeric(r.DiscountedCash),
				floatToNumeric(r.MinCharge),
				floatToNumeric(r.MaxCharge),
				modifierCodes,
				ptrToText(r.AdditionalGenericNotes),
			).Scan(&chargeID)
			if err != nil {
				return fmt.Errorf("insert charge: %w", err)
			}
			chargeCount++

			// Accumulate payer_charge rows for bulk COPY
			for _, pr := range g.rows {
				if pr.PayerName == nil {
					continue
				}
				payerName := sanitizeUTF8(*pr.PayerName)
				planName := ""
				if pr.PlanName != nil {
					planName = sanitizeUTF8(*pr.PlanName)
				}
				methodology := ""
				if pr.Methodology != nil {
					methodology = sanitizeUTF8(*pr.Methodology)
				}

				planID, ok := planCache[planName]
				if !ok {
					err := tx.QueryRow(ctx,
						`INSERT INTO plans (name) VALUES ($1)
						 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
						 RETURNING id`,
						planName,
					).Scan(&planID)
					if err != nil {
						return fmt.Errorf("upsert plan %q: %w", planName, err)
					}
					planCache[planName] = planID
				}

				pendingPayers = append(pendingPayers, []interface{}{
					chargeID,
					payerName,
					planID,
					methodology,
					floatToNumeric(pr.NegotiatedDollar),
					floatToNumeric(pr.NegotiatedPercentage),
					ptrToText(pr.NegotiatedAlgorithm),
					floatToNumeric(pr.EstimatedAmount),
					pgtype.Numeric{Valid: false}, // median_amount (not in CSV)
					pgtype.Numeric{Valid: false}, // percentile_10th
					pgtype.Numeric{Valid: false}, // percentile_90th
					pgtype.Text{Valid: false},    // count
					ptrToText(pr.AdditionalPayerNotes),
				})
			}
		}

		curItemRows = curItemRows[:0]
		txItemCount++
		return nil
	}

	// Start first transaction
	tx, err = pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for {
		n, readErr := reader.Read(buf)

		for i := 0; i < n; i++ {
			row := buf[i]
			rowsRead++

			// Insert hospital from first row
			if hospitalID == 0 {
				hospitalID, err = insertHospitalFromRow(ctx, tx, &row)
				if err != nil {
					tx.Rollback(ctx)
					return fmt.Errorf("insert hospital: %w", err)
				}
				fmt.Printf("Hospital: %s (ID: %d)\n\n", row.HospitalName, hospitalID)
			}

			ik := itemKeyOf(&row)
			if ik != curItemKey && len(curItemRows) > 0 {
				if err := flushItem(); err != nil {
					tx.Rollback(ctx)
					return err
				}

				// Commit periodically
				if txItemCount >= batchSize {
					if err := flushPayers(); err != nil {
						tx.Rollback(ctx)
						return err
					}
					if err := tx.Commit(ctx); err != nil {
						return fmt.Errorf("commit: %w", err)
					}
					tx, err = pool.Begin(ctx)
					if err != nil {
						return fmt.Errorf("begin tx: %w", err)
					}
					txItemCount = 0
				}
			}

			curItemKey = ik
			curItemRows = append(curItemRows, row)

			if time.Since(lastLog) >= 5*time.Second {
				elapsed := time.Since(start).Seconds()
				pct := float64(rowsRead) / float64(totalRows) * 100
				fmt.Printf("  progress: %d/%d rows (%.1f%%) | %d items, %d charges, %d payer (%.0f rows/s)\n",
					rowsRead, totalRows, pct, itemCount, chargeCount, payerCount, float64(rowsRead)/elapsed)
				lastLog = time.Now()
			}
		}

		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			tx.Rollback(ctx)
			return fmt.Errorf("read parquet: %w", readErr)
		}
	}

	// Flush last item
	if len(curItemRows) > 0 {
		if err := flushItem(); err != nil {
			tx.Rollback(ctx)
			return err
		}
	}

	// Flush remaining payer_charges
	if err := flushPayers(); err != nil {
		tx.Rollback(ctx)
		return err
	}

	// Final commit
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("final commit: %w", err)
	}

	elapsed := time.Since(start)
	fmt.Println()
	fmt.Printf("Done in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  Rows read:      %d\n", rowsRead)
	fmt.Printf("  Items:          %d\n", itemCount)
	fmt.Printf("  Charges:        %d\n", chargeCount)
	fmt.Printf("  Payer charges:  %d\n", payerCount)
	fmt.Printf("  Codes cached:   %d\n", len(codeCache))
	fmt.Printf("  Plans cached:   %d\n", len(planCache))
	fmt.Printf("  Throughput:     %.0f rows/s\n", float64(rowsRead)/elapsed.Seconds())

	return nil
}

func insertHospitalFromRow(ctx context.Context, tx pgx.Tx, row *HospitalChargeRow) (int32, error) {
	date, err := time.Parse("2006-01-02", row.LastUpdatedOn)
	if err != nil {
		date, err = time.Parse("01/02/2006", row.LastUpdatedOn)
		if err != nil {
			date = time.Now()
		}
	}

	var addresses []string
	if row.HospitalAddress != "" {
		addresses = []string{sanitizeUTF8(row.HospitalAddress)}
	}
	var locationNames []string
	if row.HospitalLocation != "" {
		locationNames = []string{sanitizeUTF8(row.HospitalLocation)}
	}

	var id int32
	err = tx.QueryRow(ctx,
		`INSERT INTO hospitals
		 (name, addresses, location_names, npis, license_number, license_state, version, last_updated_on, attester_name)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING id`,
		sanitizeUTF8(row.HospitalName),
		addresses,
		locationNames,
		nil, // npis not in CSV
		ptrToText(row.LicenseNumber),
		ptrToText(row.LicenseState),
		sanitizeUTF8(row.Version),
		pgtype.Date{Time: date, Valid: true},
		pgtype.Text{Valid: false}, // attester_name
	).Scan(&id)
	return id, err
}

// sanitizeUTF8 replaces invalid UTF-8 bytes with spaces.
func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, " ")
}

// collectCodes extracts all non-nil code pairs from a row.
func collectCodes(r *HospitalChargeRow) [][2]string {
	var codes [][2]string
	add := func(code *string, codeType string) {
		if code != nil && *code != "" {
			codes = append(codes, [2]string{*code, codeType})
		}
	}
	add(r.CPTCode, "CPT")
	add(r.HCPCSCode, "HCPCS")
	add(r.MSDRGCode, "MS-DRG")
	add(r.NDCCode, "NDC")
	add(r.RCCode, "RC")
	add(r.ICDCode, "ICD")
	add(r.DRGCode, "DRG")
	add(r.CDMCode, "CDM")
	add(r.LOCALCode, "LOCAL")
	add(r.APCCode, "APC")
	add(r.EAPGCode, "EAPG")
	add(r.HIPPSCode, "HIPPS")
	add(r.CDTCode, "CDT")
	add(r.RDRGCode, "R-DRG")
	add(r.SDRGCode, "S-DRG")
	add(r.APSDRGCode, "APS-DRG")
	add(r.APDRGCode, "AP-DRG")
	add(r.APRDRGCode, "APR-DRG")
	add(r.TRISDRGCode, "TRIS-DRG")
	return codes
}

// itemKeyOf returns a string identifying the standard_charge_item.
// Adjacent Parquet rows with the same item key belong to the same item.
func itemKeyOf(r *HospitalChargeRow) string {
	var b strings.Builder
	b.WriteString(r.Description)
	b.WriteByte('\t')
	codes := collectCodes(r)
	for _, c := range codes {
		b.WriteString(c[1])
		b.WriteByte(':')
		b.WriteString(c[0])
		b.WriteByte('|')
	}
	if r.DrugUnitOfMeasurement != nil {
		fmt.Fprintf(&b, "\t%.4f", *r.DrugUnitOfMeasurement)
	}
	if r.DrugTypeOfMeasurement != nil {
		b.WriteByte('\t')
		b.WriteString(*r.DrugTypeOfMeasurement)
	}
	return b.String()
}

// chargeKeyOf groups rows into standard_charges within an item.
func chargeKeyOf(r *HospitalChargeRow) string {
	var b strings.Builder
	b.WriteString(r.Setting)
	b.WriteByte('\t')
	writeOptFloat(&b, r.GrossCharge)
	b.WriteByte('\t')
	writeOptFloat(&b, r.DiscountedCash)
	b.WriteByte('\t')
	writeOptFloat(&b, r.MinCharge)
	b.WriteByte('\t')
	writeOptFloat(&b, r.MaxCharge)
	b.WriteByte('\t')
	if r.Modifiers != nil {
		b.WriteString(*r.Modifiers)
	}
	b.WriteByte('\t')
	if r.AdditionalGenericNotes != nil {
		b.WriteString(*r.AdditionalGenericNotes)
	}
	return b.String()
}

func writeOptFloat(b *strings.Builder, f *float64) {
	if f != nil {
		fmt.Fprintf(b, "%.6f", *f)
	}
}

// pgtype helpers

func floatToNumeric(f *float64) pgtype.Numeric {
	if f == nil {
		return pgtype.Numeric{Valid: false}
	}
	bf := big.NewFloat(*f)
	text := bf.Text('f', -1)
	var num pgtype.Numeric
	num.Scan(text)
	return num
}

func ptrToText(s *string) pgtype.Text {
	if s == nil || *s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: sanitizeUTF8(*s), Valid: true}
}
