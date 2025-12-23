{{ config(materialized='table') }}

SELECT
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    is_working,
    is_deleted,
    now() AS loaded_at
FROM {{ ref('raw_terminal') }}
