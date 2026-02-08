package main

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"

	"hospital_loader/db"

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
	// Payer cache: payer_name → payer_id
	payerCache := make(map[string]int32)

	const readBatch = 8192
	buf := make([]HospitalChargeRow, readBatch)

	var (
		hospitalID  int32
		tx          pgx.Tx
		q           *db.Queries
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
		pendingPayers []db.InsertPayerChargesParams
	)

	// flushPayers bulk-inserts accumulated payer_charges via COPY.
	flushPayers := func() error {
		if len(pendingPayers) == 0 {
			return nil
		}
		copied, err := q.InsertPayerCharges(ctx, pendingPayers)
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
		itemID, err := q.InsertStandardChargeItem(ctx, db.InsertStandardChargeItemParams{
			HospitalID:   hospitalID,
			Description:  sanitizeUTF8(first.Description),
			DrugUnit:     floatToNumeric(first.DrugUnitOfMeasurement),
			DrugUnitType: optToPgText(first.DrugTypeOfMeasurement),
		})
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
				codeID, err = q.UpsertCode(ctx, db.UpsertCodeParams{
					Code:     cp[0],
					CodeType: cp[1],
				})
				if err != nil {
					return fmt.Errorf("upsert code %s/%s: %w", cp[0], cp[1], err)
				}
				codeCache[cacheKey] = codeID
			}

			if err := q.InsertItemCode(ctx, db.InsertItemCodeParams{
				ItemID: itemID,
				CodeID: codeID,
			}); err != nil {
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

			chargeID, err := q.InsertStandardCharge(ctx, db.InsertStandardChargeParams{
				ItemID:          itemID,
				Setting:         r.Setting,
				GrossCharge:     floatToNumeric(r.GrossCharge),
				DiscountedCash:  floatToNumeric(r.DiscountedCash),
				Minimum:         floatToNumeric(r.MinCharge),
				Maximum:         floatToNumeric(r.MaxCharge),
				ModifierCodes:   modifierCodes,
				AdditionalNotes: optToPgText(r.AdditionalGenericNotes),
			})
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

				payerID, ok := payerCache[payerName]
				if !ok {
					payerID, err = q.UpsertPayer(ctx, payerName)
					if err != nil {
						return fmt.Errorf("upsert payer %q: %w", payerName, err)
					}
					payerCache[payerName] = payerID
				}

				planID, ok := planCache[planName]
				if !ok {
					planID, err = q.UpsertPlan(ctx, planName)
					if err != nil {
						return fmt.Errorf("upsert plan %q: %w", planName, err)
					}
					planCache[planName] = planID
				}

				pendingPayers = append(pendingPayers, db.InsertPayerChargesParams{
					StandardChargeID:         chargeID,
					PayerID:                  payerID,
					PlanID:                   planID,
					Methodology:              methodology,
					StandardChargeDollar:     floatToNumeric(pr.NegotiatedDollar),
					StandardChargePercentage: floatToNumeric(pr.NegotiatedPercentage),
					StandardChargeAlgorithm:  optToPgText(pr.NegotiatedAlgorithm),
					EstimatedAmount:          floatToNumeric(pr.EstimatedAmount),
					MedianAmount:             pgtype.Numeric{Valid: false},
					Percentile10th:           pgtype.Numeric{Valid: false},
					Percentile90th:           pgtype.Numeric{Valid: false},
					Count:                    pgtype.Text{Valid: false},
					AdditionalNotes:          optToPgText(pr.AdditionalPayerNotes),
				})
			}
		}

		curItemRows = curItemRows[:0]
		txItemCount++
		return nil
	}

	// beginTx starts a new transaction and creates a Queries wrapper.
	beginTx := func() error {
		tx, err = pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		q = db.New(tx)
		return nil
	}

	// Start first transaction
	if err := beginTx(); err != nil {
		return err
	}

	for {
		n, readErr := reader.Read(buf)

		for i := 0; i < n; i++ {
			row := buf[i]
			rowsRead++

			// Insert hospital from first row
			if hospitalID == 0 {
				hospitalID, err = insertHospitalFromRow(ctx, q, &row)
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
					if err := beginTx(); err != nil {
						return err
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
	fmt.Printf("  Payers cached:  %d\n", len(payerCache))
	fmt.Printf("  Throughput:     %.0f rows/s\n", float64(rowsRead)/elapsed.Seconds())

	return nil
}

func insertHospitalFromRow(ctx context.Context, q *db.Queries, row *HospitalChargeRow) (int32, error) {
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

	return q.InsertHospital(ctx, db.InsertHospitalParams{
		Name:          sanitizeUTF8(row.HospitalName),
		Addresses:     addresses,
		LocationNames: locationNames,
		Npis:          nil,
		LicenseNumber: optToPgText(row.LicenseNumber),
		LicenseState:  optToPgText(row.LicenseState),
		Version:       sanitizeUTF8(row.Version),
		LastUpdatedOn: pgtype.Date{Time: date, Valid: true},
		AttesterName:  pgtype.Text{Valid: false},
	})
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

func optToPgText(s *string) pgtype.Text {
	if s == nil || *s == "" {
		return pgtype.Text{Valid: false}
	}
	return pgtype.Text{String: sanitizeUTF8(*s), Valid: true}
}
