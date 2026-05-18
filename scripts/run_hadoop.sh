#!/bin/bash
echo "============================="
echo "Running Hadoop cluster"
echo "==========================="
cd infrastructure/hadoop/00-setup/hadoop
docker compose up -d