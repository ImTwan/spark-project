#!/bin/bash

echo "================================"
echo "Checking PostgreSQL tables..."
echo "================================"

docker exec -it postgres psql -U postgres -d postgres -c "\dt"

echo "================================"
echo "Check Completed"
echo "================================"
