{{ config(materialized='view') }}

SELECT
    txn_id,
    card_id,
    terminal_id,
    txn_ts,
    amount,
    currency_code,
    txn_type,
    status
FROM {{ source('bank_dwh', 'stg_transaction') }}
