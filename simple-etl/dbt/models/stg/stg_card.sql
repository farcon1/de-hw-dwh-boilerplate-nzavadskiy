{{ config(materialized='view') }}

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    created_at,
    updated_at,
    is_deleted
FROM {{ source('bank_dwh', 'stg_card') }}
