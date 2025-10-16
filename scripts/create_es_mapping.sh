#!/usr/bin/env bash
set -euo pipefail
curl -X PUT "http://localhost:9200/driver_locations" -H "Content-Type: application/json" -d '{
  "mappings": {
    "properties": {
      "driverId":     { "type": "keyword" },
      "location":     { "type": "geo_point" },
      "availability": { "type": "keyword" },
      "@timestamp":   { "type": "date" }
    }
  }
}'
echo
