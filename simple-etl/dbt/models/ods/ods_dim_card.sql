{{ config(materialized='table') }}

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    is_deleted,
    now() AS loaded_at
FROM {{ ref('raw_card') }}
