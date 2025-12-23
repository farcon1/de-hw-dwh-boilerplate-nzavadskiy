{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

WITH src AS (
    SELECT
        customer_id,
        customer_uuid,
        full_name,
        birth_date,
        email,
        phone,
        city,
        created_at,
        updated_at,
        is_deleted
    FROM {{ ref('stg_customer') }}
)

SELECT
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
