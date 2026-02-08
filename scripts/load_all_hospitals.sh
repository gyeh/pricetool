#!/bin/bash

# Script to load all hospital MRF parquet files into PostgreSQL
#
# Usage: ./load_all_hospitals.sh [options]
#
# Options:
#   --fresh         Drop and recreate the database (default: append)
#   --batch N       Batch size for loading (default: 500)
#   --pg CONNSTR    PostgreSQL connection string
#                   (default: postgres://postgres:postgres@localhost:5432/hospital_pricing)
#   --dir DIR       Parquet directory (default: testdata/hospital_mrf_parquet)
#   --file FILE     Load a single file instead of all files
#   -h, --help      Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
FRESH=false
BATCH=500
PG_CONN="postgres://postgres:postgres@localhost:5432/hospital_pricing"
PARQUET_DIR="testdata/hospital_mrf_parquet"
SINGLE_FILE=""
CONTAINER_NAME="pricetool-postgres"
USER=postgres
PASSWORD=postgres
DBNAME=hospital_pricing

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --fresh)
            FRESH=true
            shift
            ;;
        --batch)
            BATCH="$2"
            shift 2
            ;;
        --pg)
            PG_CONN="$2"
            shift 2
            ;;
        --dir)
            PARQUET_DIR="$2"
            shift 2
            ;;
        --file)
            SINGLE_FILE="$2"
            shift 2
            ;;
        -h|--help)
            head -12 "$0" | tail -10
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "============================================"
echo "Hospital MRF Parquet → PostgreSQL Loader"
echo "============================================"
echo ""

# Step 1: Ensure PostgreSQL is running
echo "[1/4] Checking PostgreSQL..."
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "  PostgreSQL container is running."
else
    echo "  Starting PostgreSQL..."
    ./start_postgres.sh
fi

if [ "$FRESH" = true ]; then
    echo "  Dropping and recreating database..."
    docker exec "$CONTAINER_NAME" psql -U "$USER" -c "DROP DATABASE IF EXISTS $DBNAME;" 2>/dev/null || true
    docker exec "$CONTAINER_NAME" psql -U "$USER" -c "CREATE DATABASE $DBNAME;" 2>/dev/null || true
fi
echo ""

# Step 2: Build tools
echo "[2/4] Building tools..."
go build -o pricetool ./parser
(cd hospital_to_duckdb && go build -o "$SCRIPT_DIR/hospital_loader" .)
echo "  Build complete."
echo ""

# Step 3: Initialize schema
echo "[3/4] Initializing database schema..."
./pricetool -init -host localhost -port 5432 -user $USER -password $PASSWORD -dbname $DBNAME
echo "  Schema initialized."
echo ""

# Step 4: Load parquet files
echo "[4/4] Loading parquet files..."
echo ""

TOTAL_FILES=0
LOADED=0
FAILED=0
SKIPPED=0

if [ -n "$SINGLE_FILE" ]; then
    FILES=("$SINGLE_FILE")
else
    FILES=()
    # Sort by size (smallest first) for faster initial progress
    while IFS= read -r line; do
        FILES+=("$line")
    done < <(ls -S -r "$PARQUET_DIR"/*.parquet 2>/dev/null)
fi

TOTAL_FILES=${#FILES[@]}

if [ "$TOTAL_FILES" -eq 0 ]; then
    echo "  No parquet files found in $PARQUET_DIR"
    exit 1
fi

echo "  Found $TOTAL_FILES parquet files"
echo "  Batch size: $BATCH"
echo ""

START_TIME=$(date +%s)

for FILE in "${FILES[@]}"; do
    LOADED=$((LOADED + 1))
    FILENAME=$(basename "$FILE")
    SIZE=$(ls -lh "$FILE" | awk '{print $5}')

    echo "────────────────────────────────────────────"
    echo "[$LOADED/$TOTAL_FILES] $FILENAME ($SIZE)"
    echo "────────────────────────────────────────────"

    if ./hospital_loader -file "$FILE" -pg "$PG_CONN" -batch "$BATCH"; then
        echo "  OK"
    else
        echo "  FAILED"
        FAILED=$((FAILED + 1))
    fi
    echo ""
done

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo "============================================"
echo "Load Complete!"
echo "============================================"
echo ""
echo "  Files loaded:  $((LOADED - FAILED)) / $TOTAL_FILES"
if [ "$FAILED" -gt 0 ]; then
    echo "  Failed:        $FAILED"
fi
echo "  Time:          ${MINUTES}m ${SECONDS}s"
echo "  Connection:    $PG_CONN"
echo ""

# Show summary
echo "Database summary:"
docker exec "$CONTAINER_NAME" psql -U "$USER" -d "$DBNAME" -c "
SELECT
    (SELECT COUNT(*) FROM hospitals) as hospitals,
    (SELECT COUNT(*) FROM codes) as codes,
    (SELECT COUNT(*) FROM standard_charge_items) as items,
    (SELECT COUNT(*) FROM standard_charges) as charges,
    (SELECT COUNT(*) FROM payer_charges) as payer_charges;
"

echo ""
echo "Hospitals loaded:"
docker exec "$CONTAINER_NAME" psql -U "$USER" -d "$DBNAME" -c "
SELECT h.id, h.name, h.version, h.last_updated_on,
       COUNT(DISTINCT sci.id) as items,
       COUNT(DISTINCT sc.id) as charges
FROM hospitals h
LEFT JOIN standard_charge_items sci ON sci.hospital_id = h.id
LEFT JOIN standard_charges sc ON sc.item_id = sci.id
GROUP BY h.id, h.name, h.version, h.last_updated_on
ORDER BY h.name;
"
