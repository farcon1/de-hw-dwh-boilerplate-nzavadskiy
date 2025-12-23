{{ config(materialized='table') }}

{% set win_sec = 600 %}
{% set min_cnt = 3 %}

WITH tx AS (
    SELECT
        txn_id,
        txn_ts,
        card_id,
        card_number,
        terminal_id,
        txn_type,
        status,
        amount,
        currency_code
    FROM {{ ref('ods_fact_atm_transaction') }}
    WHERE upper(txn_type) IN ('CASH_OUT', 'ATM_WITHDRAWAL', 'CASH_WITHDRAWAL')
),

card_scd AS (
    SELECT
        card_id,
        status,
        is_deleted,
        valid_from,
        valid_to
    FROM {{ ref('ods_dim_card_scd2') }}
),

tx_with_status AS (
    SELECT
        t.*,
        coalesce(c.status, 'UNKNOWN') AS card_status_at_txn,
        coalesce(c.is_deleted, 0)     AS card_is_deleted
    FROM tx AS t
    ANY LEFT JOIN card_scd AS c
        ON c.card_id = t.card_id
        AND toDateTime64(t.txn_ts, 3) >= c.valid_from
        AND toDateTime64(t.txn_ts, 3) <  c.valid_to
),

base AS (
    SELECT *
    FROM tx_with_status
    WHERE
        upper(status) != 'SUCCESS'
        AND (
            upper(card_status_at_txn) IN ('BLOCKED', 'CLOSED')
            OR card_is_deleted = 1
        )
),

per_card AS (
    SELECT
        card_id,
        any(card_number) AS card_number,
        arraySort(groupArray((txn_ts, txn_id, terminal_id, card_status_at_txn))) AS ev
    FROM base
    GROUP BY card_id
),

series AS (
    SELECT
        card_id,
        card_number,
        arrayMap(x -> x.1, ev) AS ts_arr,
        arrayMap(x -> x.2, ev) AS id_arr,
        arrayMap(x -> x.3, ev) AS term_arr,
        arrayMap(x -> x.4, ev) AS st_arr
    FROM per_card
),

hits AS (
    SELECT
        card_id,
        card_number,
        ts_arr,
        id_arr,
        term_arr,
        st_arr,
        if(length(candidates) = 0, 0, arrayMin(candidates)) AS first_idx
    FROM (
        SELECT
            card_id,
            card_number,
            ts_arr,
            id_arr,
            term_arr,
            st_arr,
            arrayFilter(
                i ->
                (
                    length(
                        arrayFilter(
                            j -> j >= i AND dateDiff('second', ts_arr[i], ts_arr[j]) BETWEEN 0 AND {{ win_sec }},
                            arrayEnumerate(ts_arr)
                        )
                    ) >= {{ min_cnt }}
                ),
                arrayEnumerate(ts_arr)
            ) AS candidates
        FROM series
    )
),

final AS (
    SELECT
        card_id,
        card_number,
        first_idx,
        ts_arr[first_idx] AS fraud_ts,
        toDate(ts_arr[first_idx]) AS fraud_dt,
        id_arr[first_idx] AS fraud_txn_id,
        term_arr[first_idx] AS first_terminal_id,
        st_arr[first_idx] AS card_status_at_txn,
        'blocked_card_attempts' AS fraud_type,
        now() AS loaded_at
    FROM hits
    WHERE first_idx > 0
)

SELECT
    fraud_dt,
    fraud_ts,
    fraud_txn_id,
    card_id,
    card_number,
    first_terminal_id AS terminal_id,
    card_status_at_txn,
    fraud_type,
    loaded_at
FROM final
