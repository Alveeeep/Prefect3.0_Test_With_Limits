#!/bin/bash

mkdir -p /app/prefect_data

echo "Starting Prefect Server"
prefect server start --host 0.0.0.0 &
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server database reset -y

sleep 10

echo "Creating work pool, work queue, and worker..."

prefect work-pool create "process-work-pool" --type "process"

prefect work-pool set-concurrency-limit "process-work-pool" 1

prefect gcl create "symbol-proccessing" --limit 1 --slot-decay-per-second 0.0333

prefect work-queue create "api-queue" --limit 1 --pool "process-work-pool"

python main.py

sleep 5

prefect deployment run 'main-flow/main-flow-deployment'

prefect worker start -p "process-work-pool" -q "api-queue" --limit 1
