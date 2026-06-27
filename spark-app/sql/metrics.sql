-- 1) Number of open orders by delivery date and status
SELECT delivery_date, status, open_orders
FROM analytics.mart_open_orders_by_delivery_status
ORDER BY delivery_date, status;

-- 2) Top 3 delivery dates with more open orders
SELECT rank_position, delivery_date, open_orders
FROM analytics.mart_top3_delivery_dates_open_orders
ORDER BY rank_position;

-- 3) Number of open pending items by product_id
SELECT product_id, pending_items
FROM analytics.mart_open_pending_items_by_product
ORDER BY pending_items DESC, product_id;

-- 4) Top 3 customers with more pending orders
SELECT rank_position, customer_id, pending_orders
FROM analytics.mart_top3_customers_pending_orders
ORDER BY rank_position;
