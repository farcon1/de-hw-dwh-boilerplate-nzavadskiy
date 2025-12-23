{{ config(materialized='incremental', unique_key='txn_id') }}

WITH events AS (
  SELECT
    txn_id,
    payload_after,
    src_ts,
    kafka_partition,
    kafka_offset
  FROM {{ source('bank_dwh', 'stg_cdc_transaction') }}
  WHERE op IN ('c')
),

parsed AS (
  SELECT
    txn_id,
    JSONExtractInt(payload_after, 'card_id') AS card_id,
    JSONExtractInt(payload_after, 'terminal_id') AS terminal_id,
    parseDateTimeBestEffort(JSONExtractString(payload_after, 'txn_ts')) AS txn_ts,
    toDecimal64(JSONExtractFloat(payload_after, 'amount'), 2) AS amount,
    JSONExtractString(payload_after, 'currency_code') AS currency_code,
    JSONExtractString(payload_after, 'txn_type') AS txn_type,
    JSONExtractString(payload_after, 'status') AS status,
    src_ts,
    kafka_partition,
    kafka_offset
  FROM events
)

SELECT
  txn_id, card_id, terminal_id, txn_ts, amount,
  currency_code, txn_type, status,
  now() AS loaded_at
FROM parsed

{% if is_incremental() %}
WHERE txn_id > (SELECT max(txn_id) FROM {{ this }})
{% endif %}
