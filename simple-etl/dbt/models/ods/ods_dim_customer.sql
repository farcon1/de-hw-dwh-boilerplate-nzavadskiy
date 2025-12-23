{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    is_deleted,
    now() AS loaded_at
FROM {{ ref('raw_customer') }}
