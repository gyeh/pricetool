#!/bin/bash

# Script to create and start a new PostgreSQL server using Docker
#
# Usage: ./start_postgres.sh [options]
#
# Options:
#   -p, --port      PostgreSQL port (default: 5432)
#   -u, --user      PostgreSQL user (default: postgres)
#   -w, --password  PostgreSQL password (default: postgres)
#   -d, --dbname    PostgreSQL database name (default: hospital_pricing)
#   -n, --name      Docker container name (default: pricetool-postgres)
#   --stop          Stop and remove the container
#   --status        Check container status
#   -h, --help      Show this help message
#
# Examples:
#   ./start_postgres.sh
#   ./start_postgres.sh -p 5433 -w mysecretpassword
#   ./start_postgres.sh --stop
#   ./start_postgres.sh --status

set -e

# Defaults
PORT=5432
USER=postgres
PASSWORD=postgres
DBNAME=hospital_pricing
CONTAINER_NAME=pricetool-postgres

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -u|--user)
            USER="$2"
            shift 2
            ;;
        -w|--password)
            PASSWORD="$2"
            shift 2
            ;;
        -d|--dbname)
            DBNAME="$2"
            shift 2
            ;;
        -n|--name)
            CONTAINER_NAME="$2"
            shift 2
            ;;
        --stop)
            echo "Stopping and removing container '$CONTAINER_NAME'..."
            docker stop "$CONTAINER_NAME" 2>/dev/null || true
            docker rm "$CONTAINER_NAME" 2>/dev/null || true
            echo "Done."
            exit 0
            ;;
        --status)
            if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
                echo "Container '$CONTAINER_NAME' is running"
                docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
            elif docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
                echo "Container '$CONTAINER_NAME' exists but is not running"
                docker ps -a --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}"
            else
                echo "Container '$CONTAINER_NAME' does not exist"
            fi
            exit 0
            ;;
        -h|--help)
            head -20 "$0" | tail -18
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed or not in PATH"
    exit 1
fi

# Check if container already exists
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container '$CONTAINER_NAME' is already running"
        echo "Connection: postgres://$USER:$PASSWORD@localhost:$PORT/$DBNAME"
        exit 0
    else
        echo "Starting existing container '$CONTAINER_NAME'..."
        docker start "$CONTAINER_NAME"
        echo "Waiting for PostgreSQL to be ready..."
        sleep 2
        echo "Container started!"
        echo "Connection: postgres://$USER:$PASSWORD@localhost:$PORT/$DBNAME"
        exit 0
    fi
fi

echo "Creating new PostgreSQL container..."
echo "  Container: $CONTAINER_NAME"
echo "  Port: $PORT"
echo "  User: $USER"
echo "  Database: $DBNAME"

docker run -d \
    --name "$CONTAINER_NAME" \
    -e POSTGRES_USER="$USER" \
    -e POSTGRES_PASSWORD="$PASSWORD" \
    -e POSTGRES_DB="$DBNAME" \
    -p "$PORT:5432" \
    -v "${CONTAINER_NAME}-data:/var/lib/postgresql/data" \
    postgres:16

echo "Waiting for PostgreSQL to be ready..."
until docker exec "$CONTAINER_NAME" pg_isready -U "$USER" -d "$DBNAME" > /dev/null 2>&1; do
    sleep 1
done

echo ""
echo "PostgreSQL is ready!"
echo ""
echo "Connection details:"
echo "  Host:     localhost"
echo "  Port:     $PORT"
echo "  User:     $USER"
echo "  Password: $PASSWORD"
echo "  Database: $DBNAME"
echo ""
echo "Connection string: postgres://$USER:$PASSWORD@localhost:$PORT/$DBNAME"
echo ""
echo "To upload Lenox Hill data:"
echo "  ./upload_lenox_hill.sh -user $USER -password $PASSWORD -port $PORT -dbname $DBNAME -init"
echo ""
echo "To stop the server:"
echo "  ./start_postgres.sh --stop"
