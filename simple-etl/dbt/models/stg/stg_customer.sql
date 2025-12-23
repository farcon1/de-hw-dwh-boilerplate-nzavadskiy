{{ config(materialized='view') }}

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
FROM {{ source('bank_dwh', 'stg_customer') }}
