#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

detach_mode="${1:-}"

docker compose up -d kafka zookeeper kafka-connect debezium-init transactions-db analytics-db spark >/dev/null

docker compose exec spark bash -lc "mkdir -p /workspace/.spark-checkpoints"

topics=(
  "finance_db.operations.customers"
  "finance_db.operations.products"
  "finance_db.operations.orders"
  "finance_db.operations.order_items"
)

for topic in "${topics[@]}"; do
  docker compose exec kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 >/dev/null
done

if [[ "$detach_mode" == "--detach" ]]; then
  docker compose exec -d spark spark-submit \
    --master local[*] \
    /workspace/spark-app/jobs/main.py

  echo "Pipeline started in background."
  echo "Use: docker compose logs -f spark"
  echo "Stop with: docker compose restart spark"
else
  docker compose exec spark spark-submit \
    --master local[*] \
    /workspace/spark-app/jobs/main.py
fi
