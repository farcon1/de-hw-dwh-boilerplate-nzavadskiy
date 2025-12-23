{{ config(
    materialized='incremental',
    unique_key='account_id'
) }}

WITH src AS (
    SELECT
        account_id,
        account_number,
        customer_id,
        currency_code,
        opened_at,
        closed_at,
        status,
        daily_transfer_limit,
        created_at,
        updated_at,
        is_deleted
    FROM {{ ref('stg_account') }}
)

SELECT
    account_id,
    account_number,
    customer_id,
    currency_code,
    opened_at,
    closed_at,
    status,
    daily_transfer_limit,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}
