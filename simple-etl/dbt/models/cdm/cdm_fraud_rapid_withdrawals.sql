{{ config(materialized='table') }}

{% set win_sec = 1200 %}
{% set min_cnt = 4 %}
{% set min_terminals = 3 %}

WITH base AS (
    SELECT
        txn_id,
        txn_ts,
        card_id,
        card_number,
        terminal_id,
        amount,
        currency_code,
        txn_type,
        status
    FROM {{ ref('ods_fact_atm_transaction') }}
    WHERE
        upper(txn_type) IN ('CASH_OUT', 'ATM_WITHDRAWAL', 'CASH_WITHDRAWAL')
        AND upper(status) = 'SUCCESS'
),

per_card AS (
    SELECT
        card_id,
        any(card_number) AS card_number,
        arraySort(groupArray((txn_ts, txn_id, terminal_id, amount, currency_code))) AS ev
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
        arrayMap(x -> x.4, ev) AS amt_arr,
        arrayMap(x -> x.5, ev) AS cur_arr
    FROM per_card
),

hits AS (
    SELECT
        card_id,
        card_number,
        ts_arr,
        id_arr,
        term_arr,
        amt_arr,
        cur_arr,
        if(length(candidates) = 0, 0, arrayMin(candidates)) AS first_idx
    FROM (
        SELECT
            card_id,
            card_number,
            ts_arr,
            id_arr,
            term_arr,
            amt_arr,
            cur_arr,
            arrayFilter(
                i ->
                (
                    length(win_idx) >= {{ min_cnt }}
                    AND length(arrayDistinct(arrayMap(j -> term_arr[j], win_idx))) >= {{ min_terminals }}
                ),
                arrayEnumerate(ts_arr)
            ) AS candidates
        FROM (
            SELECT
                card_id,
                card_number,
                ts_arr,
                id_arr,
                term_arr,
                amt_arr,
                cur_arr,
                arrayMap(
                    i ->
                    arrayFilter(
                        j -> j >= i AND dateDiff('second', ts_arr[i], ts_arr[j]) BETWEEN 0 AND {{ win_sec }},
                        arrayEnumerate(ts_arr)
                    ),
                    arrayEnumerate(ts_arr)
                ) AS win_idx_by_i,
                [] AS win_idx
            FROM series
        )
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
        'rapid_withdrawals' AS fraud_type,
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
    fraud_type,
    loaded_at
FROM final
