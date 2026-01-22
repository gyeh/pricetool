package main

import (
	"io"
	"testing"
)

func TestCSVStreamReaderTallGrouping(t *testing.T) {
	reader, err := NewCSVStreamReader("../testdata/V2.0.0_Tall_CSV_Format_Example.csv")
	if err != nil {
		t.Fatalf("failed to create stream reader: %v", err)
	}
	defer reader.Close()

	_, err = reader.ReadHeader()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// Count unique items vs total payer entries
	var itemCount int
	var totalPayers int
	for {
		item, err := reader.NextItem()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("failed to read item: %v", err)
		}
		itemCount++
		if len(item.StandardCharges) > 0 {
			totalPayers += len(item.StandardCharges[0].PayersInformation)
		}
	}

	// There are 31 raw rows but they group into fewer unique items
	// Each item can have multiple payers
	if itemCount >= 31 {
		t.Errorf("expected grouping to reduce item count below 31, got %d", itemCount)
	}
	if totalPayers < itemCount {
		t.Errorf("expected total payers (%d) >= item count (%d)", totalPayers, itemCount)
	}
}

func TestCSVStreamReaderRowNum(t *testing.T) {
	reader, err := NewCSVStreamReader("../testdata/V2.0.0_Wide_CSV_Format_Example.csv")
	if err != nil {
		t.Fatalf("failed to create stream reader: %v", err)
	}
	defer reader.Close()

	if reader.RowNum() != 0 {
		t.Errorf("expected initial row num 0, got %d", reader.RowNum())
	}

	_, err = reader.ReadHeader()
	if err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// After reading header (3 rows), should be at row 3
	if reader.RowNum() != 3 {
		t.Errorf("expected row num 3 after header, got %d", reader.RowNum())
	}

	// Read one item
	_, err = reader.NextItem()
	if err != nil {
		t.Fatalf("failed to read item: %v", err)
	}

	// Should be at row 4
	if reader.RowNum() != 4 {
		t.Errorf("expected row num 4 after first item, got %d", reader.RowNum())
	}
}
