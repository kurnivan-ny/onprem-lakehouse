-- =====================================================================================
-- orders-pipeline : Kafka -> Iceberg (bronze / silver / gold) + DLQ, one Flink job.
-- Placeholders __LIKE_THIS__ are rendered by submit.sh. Validation rules mirror
-- contracts/orders.v1.json - change both together.
-- =====================================================================================

SET 'pipeline.name' = 'orders-pipeline';
SET 'execution.checkpointing.interval' = '30s';   -- Iceberg commits + Kafka offsets on each checkpoint
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '7 d';               -- bounds state for the current-status aggregation
SET 'table.exec.source.idle-timeout' = '10 s';    -- idle partitions don't stall watermarks

-- ---------- catalog: Lakekeeper (Iceberg REST) + RustFS ----------
CREATE CATALOG lakehouse WITH (
  'type'                 = 'iceberg',
  'catalog-type'         = 'rest',
  'uri'                  = 'http://lakekeeper:8181/catalog',
  'warehouse'            = '__WAREHOUSE__',
  'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint'          = 'http://rustfs:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id'     = '__S3_ACCESS_KEY__',
  's3.secret-access-key' = '__S3_SECRET_KEY__',
  'client.region'        = 'us-east-1'
);

CREATE DATABASE IF NOT EXISTS lakehouse.bronze;
CREATE DATABASE IF NOT EXISTS lakehouse.silver;
CREATE DATABASE IF NOT EXISTS lakehouse.gold;

-- ---------- Iceberg tables ----------
CREATE TABLE IF NOT EXISTS lakehouse.bronze.orders_events_raw (
  `value`          STRING,
  kafka_partition  INT,
  kafka_offset     BIGINT,
  kafka_ts         TIMESTAMP_LTZ(6),
  ingested_at      TIMESTAMP_LTZ(6),
  ingest_date      STRING
) PARTITIONED BY (ingest_date)
  WITH ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lakehouse.silver.orders_events (
  event_date      STRING,
  event_id        STRING,
  event_type      STRING,
  event_ts        TIMESTAMP_LTZ(6),
  order_id        STRING,
  customer_id     BIGINT,
  country         STRING,
  payment_method  STRING,
  amount          DOUBLE,
  currency        STRING,
  schema_version  INT,
  kafka_ts        TIMESTAMP_LTZ(6),
  ingested_at     TIMESTAMP_LTZ(6),
  PRIMARY KEY (event_date, event_id) NOT ENFORCED   -- upsert = dedup of replays/duplicates
) PARTITIONED BY (event_date)
  WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_current_status (
  order_id        STRING,
  status          STRING,
  customer_id     BIGINT,
  country         STRING,
  amount          DOUBLE,
  first_event_ts  TIMESTAMP_LTZ(6),
  last_event_ts   TIMESTAMP_LTZ(6),
  events          BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('format-version' = '2', 'write.upsert.enabled' = 'true');

CREATE TABLE IF NOT EXISTS lakehouse.gold.orders_revenue_per_minute (
  window_start  TIMESTAMP(6),      -- UTC
  window_end    TIMESTAMP(6),      -- UTC
  country       STRING,
  paid_orders   BIGINT,
  revenue       DOUBLE
) WITH ('format-version' = '2');

-- ---------- Kafka source + DLQ sink (session-scoped) ----------
CREATE TEMPORARY TABLE kafka_orders (
  `value`          STRING,                                       -- raw bytes as text: bad JSON never crashes the job
  kafka_partition  INT     METADATA FROM 'partition' VIRTUAL,
  kafka_offset     BIGINT  METADATA FROM 'offset' VIRTUAL,
  kafka_ts         TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR kafka_ts AS kafka_ts - INTERVAL '5' SECOND
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = '__TOPIC_EVENTS__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id'          = 'flink-lakehouse',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode'            = 'group-offsets',
  'format'                       = 'raw'
);

CREATE TEMPORARY TABLE kafka_dlq (
  error             STRING,
  raw_value         STRING,
  source_topic      STRING,
  source_partition  INT,
  source_offset     BIGINT,
  failed_at         TIMESTAMP_LTZ(3)
) WITH (
  'connector'                    = 'kafka',
  'topic'                        = '__TOPIC_DLQ__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format'                       = 'json'
);

-- ---------- parse + validate (contract orders.v1) ----------
CREATE TEMPORARY VIEW orders_parsed AS
SELECT
  `value`, kafka_partition, kafka_offset, kafka_ts,
  JSON_VALUE(`value`, '$.event_id')                              AS event_id,
  JSON_VALUE(`value`, '$.event_type')                            AS event_type,
  CAST(TO_TIMESTAMP(REPLACE(REPLACE(JSON_VALUE(`value`, '$.event_ts'), 'T', ' '), 'Z', ''),
                    'yyyy-MM-dd HH:mm:ss.SSS') AS TIMESTAMP_LTZ(6)) AS event_ts,
  JSON_VALUE(`value`, '$.order_id')                              AS order_id,
  JSON_VALUE(`value`, '$.customer_id' RETURNING BIGINT)          AS customer_id,
  JSON_VALUE(`value`, '$.country')                               AS country,
  COALESCE(JSON_VALUE(`value`, '$.payment_method'), 'unknown')   AS payment_method,
  JSON_VALUE(`value`, '$.amount' RETURNING DOUBLE)               AS amount,
  COALESCE(JSON_VALUE(`value`, '$.currency'), 'unknown')         AS currency,
  COALESCE(JSON_VALUE(`value`, '$.schema_version' RETURNING INT), 1) AS schema_version
FROM kafka_orders;

CREATE TEMPORARY VIEW orders_checked AS
SELECT *,
  CASE
    WHEN `value` IS NULL OR NOT (`value` IS JSON OBJECT)                THEN 'invalid_json'
    WHEN event_id IS NULL                                               THEN 'missing_event_id'
    WHEN order_id IS NULL                                               THEN 'missing_order_id'
    WHEN customer_id IS NULL                                            THEN 'missing_customer_id'
    WHEN country IS NULL OR CHAR_LENGTH(country) <> 2                   THEN 'bad_country'
    WHEN event_type IS NULL OR event_type NOT IN
         ('order_created', 'order_paid', 'order_shipped', 'order_cancelled') THEN 'unknown_event_type'
    WHEN amount IS NULL                                                 THEN 'missing_amount'
    WHEN amount < 0                                                     THEN 'negative_amount'
    WHEN event_ts IS NULL                                               THEN 'bad_event_ts'
    ELSE CAST(NULL AS STRING)
  END AS error
FROM orders_parsed;

-- paid orders per minute, deduped per order inside the window (cascading window aggregation)
CREATE TEMPORARY VIEW paid_per_order AS
SELECT window_start, window_end, window_time AS rowtime, country, order_id, MAX(amount) AS amount
FROM TABLE(TUMBLE(TABLE orders_checked, DESCRIPTOR(kafka_ts), INTERVAL '1' MINUTE))
WHERE error IS NULL AND event_type = 'order_paid'
GROUP BY window_start, window_end, window_time, country, order_id;

-- ---------- one job, five sinks ----------
EXECUTE STATEMENT SET
BEGIN

  -- bronze: every record, untouched (replayable source of truth)
  INSERT INTO lakehouse.bronze.orders_events_raw
  SELECT `value`, kafka_partition, kafka_offset,
         CAST(kafka_ts AS TIMESTAMP_LTZ(6)),
         CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(6)),
         DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
  FROM kafka_orders;

  -- silver: valid, typed, deduped by (event_date, event_id)
  INSERT INTO lakehouse.silver.orders_events
  SELECT DATE_FORMAT(event_ts, 'yyyy-MM-dd'), event_id, event_type, event_ts, order_id, customer_id,
         country, payment_method, amount, currency, schema_version,
         CAST(kafka_ts AS TIMESTAMP_LTZ(6)), CAST(CURRENT_TIMESTAMP AS TIMESTAMP_LTZ(6))
  FROM orders_checked
  WHERE error IS NULL;

  -- DLQ: invalid records + reason, back to Kafka for inspection / replay
  INSERT INTO kafka_dlq
  SELECT error, `value`, '__TOPIC_EVENTS__', kafka_partition, kafka_offset, CURRENT_TIMESTAMP
  FROM orders_checked
  WHERE error IS NOT NULL;

  -- gold: latest state per order (upsert on order_id)
  INSERT INTO lakehouse.gold.orders_current_status
  SELECT order_id,
         REPLACE(LAST_VALUE(event_type), 'order_', ''),
         MAX(customer_id), MAX(country), MAX(amount),
         MIN(event_ts), MAX(event_ts),
         COUNT(DISTINCT event_id)
  FROM orders_checked
  WHERE error IS NULL
  GROUP BY order_id;

  -- gold: paid revenue per minute x country (event-time windows, append-only)
  INSERT INTO lakehouse.gold.orders_revenue_per_minute
  SELECT CAST(window_start AS TIMESTAMP(6)), CAST(window_end AS TIMESTAMP(6)), country,
         COUNT(*), ROUND(SUM(amount), 2)
  FROM TABLE(TUMBLE(TABLE paid_per_order, DESCRIPTOR(rowtime), INTERVAL '1' MINUTE))
  GROUP BY window_start, window_end, country;

END;
