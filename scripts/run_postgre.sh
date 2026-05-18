#!/bin/bash

echo "================================"
echo "Starting PostgreSQL..."
echo "================================"

cd infrastructure/postgres/00-setup/postgres || exit

docker compose up -d

echo "================================"
echo "PostgreSQL Started"
echo "================================"
