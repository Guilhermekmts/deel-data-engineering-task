#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <metric>"
  echo "Metrics: open_orders_by_delivery_status | top3_delivery_dates | pending_items_by_product | top3_customers_pending_orders"
  exit 1
fi

metric="$1"

case "$metric" in
  open_orders_by_delivery_status)
    sql="SELECT delivery_date, status, open_orders, updated_at FROM analytics.mart_open_orders_by_delivery_status ORDER BY delivery_date, status;"
    ;;
  top3_delivery_dates)
    sql="SELECT rank_position, delivery_date, open_orders, updated_at FROM analytics.mart_top3_delivery_dates_open_orders ORDER BY rank_position;"
    ;;
  pending_items_by_product)
    sql="SELECT product_id, pending_items, updated_at FROM analytics.mart_open_pending_items_by_product ORDER BY pending_items DESC, product_id;"
    ;;
  top3_customers_pending_orders)
    sql="SELECT rank_position, customer_id, pending_orders, updated_at FROM analytics.mart_top3_customers_pending_orders ORDER BY rank_position;"
    ;;
  *)
    echo "Invalid metric: $metric"
    exit 1
    ;;
esac

docker compose exec analytics-db psql -U analytics_user -d analytics_db -c "$sql"
