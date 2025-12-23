{{ config(materialized='table') }}

WITH ev AS (
  SELECT
    card_id,
    op,
    payload_after,
    payload_before,
    src_ts,
    kafka_partition,
    kafka_offset
  FROM {{ source('bank_dwh', 'stg_cdc_card') }}
  WHERE op IN ('r','c','u','d')
),

-- выбираем "состояние после" (after), а если delete — берём before и помечаем deleted
snap AS (
  SELECT
    card_id,
    -- для delete after может быть NULL, берём before
    if(op = 'd', payload_before, payload_after) AS payload,
    if(op = 'd', 1, 0) AS deleted_by_op,
    src_ts,
    kafka_partition,
    kafka_offset
  FROM ev
),

parsed AS (
  SELECT
    card_id,
    JSONExtractString(payload, 'card_number') AS card_number,
    toInt32(JSONExtractInt(payload, 'account_id')) AS account_id,
    JSONExtractString(payload, 'status') AS status,
    parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'opened_at')) AS opened_at,
    parseDateTimeBestEffortOrNull(JSONExtractString(payload, 'closed_at')) AS closed_at,
    -- is_deleted в источнике + delete-op
    greatest(toUInt8(JSONExtractInt(payload, 'is_deleted')), toUInt8(deleted_by_op)) AS is_deleted,
    src_ts,
    kafka_partition,
    kafka_offset
  FROM snap
),

ordered AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY card_id
      ORDER BY src_ts, kafka_partition, kafka_offset
    ) AS rn,
    lead(src_ts, 1) OVER (
      PARTITION BY card_id
      ORDER BY src_ts, kafka_partition, kafka_offset
    ) AS next_ts
  FROM parsed
)

SELECT
  -- surrogate key (детерминированный)
  cityHash64(toString(card_id), toString(src_ts), toString(kafka_partition), toString(kafka_offset)) AS card_sk,

  card_id,
  card_number,
  account_id,
  status,
  opened_at,
  closed_at,
  is_deleted,

  src_ts AS valid_from,
  coalesce(next_ts, toDateTime64('2999-12-31 00:00:00', 3)) AS valid_to,
  if(next_ts IS NULL, 1, 0) AS is_current,

  src_ts,
  now() AS loaded_at
FROM ordered
