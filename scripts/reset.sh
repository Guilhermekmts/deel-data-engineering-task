#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

echo "Stopping stack and removing containers/networks/volumes..."
docker compose down -v --remove-orphans

echo "Removing Spark checkpoints and Delta data..."
rm -rf .spark-checkpoints data/delta

echo "Rebuilding images (no cache)..."
docker compose build --no-cache

echo "Starting clean stack..."
docker compose up -d

echo "Re-registering Debezium connectors..."
docker compose run --rm debezium-init

echo "Starting Spark pipeline in detached mode..."
./scripts/run_pipeline.sh --detach