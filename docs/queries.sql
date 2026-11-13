-- Sample Trino queries over the lakehouse (make sql-samples, or paste into Superset SQL Lab).

-- What exists?
SHOW SCHEMAS FROM lakehouse;
SHOW TABLES FROM lakehouse.gold;

-- Batch gold: revenue by country (WIB dates)
SELECT country, round(sum(revenue), 2) AS revenue, sum(orders) AS orders
FROM lakehouse.gold.sales_daily
GROUP BY country ORDER BY revenue DESC;

-- Event gold: last 10 minutes of paid revenue
SELECT window_start, sum(revenue) AS revenue, sum(paid_orders) AS paid_orders
FROM lakehouse.gold.orders_revenue_per_minute
GROUP BY window_start ORDER BY window_start DESC LIMIT 10;

-- Event gold: current order status mix
SELECT status, count(*) AS orders FROM lakehouse.gold.orders_current_status GROUP BY status;

-- Dedup + DLQ effect: raw records vs unique valid events
SELECT (SELECT count(*) FROM lakehouse.bronze.orders_events_raw) AS bronze_records,
       (SELECT count(*) FROM lakehouse.silver.orders_events)     AS silver_unique_events;

-- Reconciliation: source vs silver (needs batch profile)
SELECT (SELECT count(*) FROM oltp.sales.orders)            AS source_orders,
       (SELECT count(*) FROM lakehouse.silver.sales_orders) AS silver_orders;

-- Iceberg time travel: table history
SELECT snapshot_id, committed_at, operation FROM lakehouse.silver."sales_orders$snapshots" ORDER BY committed_at DESC LIMIT 5;
