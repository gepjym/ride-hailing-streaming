#!/bin/bash

# Wait for Kibana to be ready
until curl -s http://localhost:5601/api/status | grep -q '"level":"available"'; do
    echo "Waiting for Kibana..."
    sleep 5
done

# Create index pattern for driver_locations
curl -X POST "http://localhost:5601/api/saved_objects/index-pattern/driver-locations-pattern" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "driver_locations*",
      "timeFieldName": "@timestamp"
    }
  }'

# Create index pattern for timeseries
curl -X POST "http://localhost:5601/api/saved_objects/index-pattern/driver-locations-timeseries-pattern" \
  -H "kbn-xsrf: true" \
  -H "Content-Type: application/json" \
  -d '{
    "attributes": {
      "title": "driver_locations_timeseries-*",
      "timeFieldName": "@timestamp"
    }
  }'

echo "Index patterns created successfully"
