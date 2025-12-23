{{ config(
    materialized='incremental',
    unique_key='card_id'
) }}

WITH src AS (
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
    FROM {{ ref('stg_card') }}
)

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
