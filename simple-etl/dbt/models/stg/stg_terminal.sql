{{ config(materialized='view') }}

SELECT
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    created_at,
    updated_at,
    is_working,
    is_deleted
FROM {{ source('bank_dwh', 'stg_terminal') }}
