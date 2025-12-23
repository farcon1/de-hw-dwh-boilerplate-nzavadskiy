{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

WITH src AS (
    SELECT
        txn_id,
        card_id,
        terminal_id,
        txn_ts,
        amount,
        currency_code,
        txn_type,
        status
    FROM {{ ref('stg_transaction') }}
)

SELECT
    txn_id,
    card_id,
    terminal_id,
    txn_ts,
    amount,
    currency_code,
    txn_type,
    status,
    now() AS loaded_at
FROM src

{% if is_incremental() %}
WHERE txn_id > (SELECT max(txn_id) FROM {{ this }})
{% endif %}
