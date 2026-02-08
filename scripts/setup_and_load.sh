#!/bin/bash

# Script to set up a fresh PostgreSQL database and load Mount Sinai hospital data
#
# Usage: ./setup_and_load.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
CONTAINER_NAME="pricetool-postgres"
PORT=5432
USER=postgres
PASSWORD=postgres
DBNAME=hospital_pricing

echo "========================================"
echo "PriceTool Database Setup & Load Script"
echo "========================================"
echo ""

# Step 1: Stop existing container and remove volume for fresh start
echo "[1/5] Stopping existing PostgreSQL container..."
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true
docker volume rm "${CONTAINER_NAME}-data" 2>/dev/null || true
echo "  Done."
echo ""

# Step 2: Start fresh PostgreSQL container
echo "[2/5] Starting fresh PostgreSQL container..."
docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_USER="$USER" \
    -e POSTGRES_PASSWORD="$PASSWORD" \
    -e POSTGRES_DB="$DBNAME" \
    -p "$PORT:5432" \
    -v "${CONTAINER_NAME}-data:/var/lib/postgresql/data" \
    postgres:16

echo "  Waiting for PostgreSQL to be ready..."
until docker exec "$CONTAINER_NAME" pg_isready -U "$USER" -d "$DBNAME" > /dev/null 2>&1; do
    sleep 1
done
echo "  PostgreSQL is ready!"
echo ""

# Step 3: Build pricetool binary
echo "[3/5] Building pricetool binary..."
go build -o pricetool ./parser
echo "  Build complete."
echo ""

# Step 4: Initialize schema
echo "[4/5] Initializing database schema..."
./pricetool -init -host localhost -port $PORT -user $USER -password $PASSWORD -dbname $DBNAME
echo "  Schema initialized."
echo ""

# Step 5: Load hospital data files
echo "[5/5] Loading hospital data files..."
echo ""

FILES=(
    "testdata/111352310_mount-sinai-south-nassau_standardcharges.json"
    "testdata/132997301_mount-sinai-morningside_standardcharges.json"
    "testdata/131624096_mount-sinai-queens_standardcharges.json"
    "testdata/131624096_mount-sinai-hospital_standardcharges.json"
    "testdata/135564934_mount-sinai-brooklyn_standardcharges.json"
    "testdata/133971298-1801992631_nyu-langone-tisch_standardcharges.csv"
)

for FILE in "${FILES[@]}"; do
    if [ -f "$FILE" ]; then
        FILENAME=$(basename "$FILE")
        SIZE=$(ls -lh "$FILE" | awk '{print $5}')
        echo "  Loading: $FILENAME ($SIZE)"
        ./pricetool -file "$FILE" -host localhost -port $PORT -user $USER -password $PASSWORD -dbname $DBNAME -batch 2000
        echo ""
    else
        echo "  WARNING: File not found: $FILE"
    fi
done

echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Database connection:"
echo "  postgres://$USER:$PASSWORD@localhost:$PORT/$DBNAME"
echo ""
echo "To start the web frontend:"
echo "  cd web && npm run dev"
echo ""
echo "To stop the database:"
echo "  ./start_postgres.sh --stop"
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
