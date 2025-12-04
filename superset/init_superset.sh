#!/bin/bash
set -e

echo "Waiting for Superset to be ready..."
sleep 30

# Add database connection
docker exec -i superset superset set_database_uri \
  --database_name "Reporting DB" \
  --uri "postgresql://user:password@postgres-reporting:5432/reporting_db"

echo "Database connection added successfully"
