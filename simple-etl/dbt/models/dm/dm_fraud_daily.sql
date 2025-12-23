{{ config(materialized='table') }}

WITH all_frauds AS (
    SELECT fraud_dt, fraud_type FROM {{ ref('cdm_fraud_rapid_withdrawals') }}
    UNION ALL
    SELECT fraud_dt, fraud_type FROM {{ ref('cdm_fraud_blocked_card_attempts') }}
    UNION ALL
    SELECT fraud_dt, fraud_type FROM {{ ref('cdm_fraud_amount_probe') }}
)

SELECT
    fraud_dt,
    fraud_type,
    count() AS fraud_cnt
FROM all_frauds
GROUP BY fraud_dt, fraud_type
ORDER BY fraud_dt, fraud_type
