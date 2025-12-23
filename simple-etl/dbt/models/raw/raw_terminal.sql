{{ config(
    materialized='incremental',
    unique_key='terminal_id'
) }}

WITH src AS (
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
    FROM {{ ref('stg_terminal') }}
)

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
    is_deleted,
    now() AS loaded_at
FROM src

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
