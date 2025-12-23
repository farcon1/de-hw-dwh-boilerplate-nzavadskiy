import os
from datetime import datetime

import pendulum
import psycopg2
from psycopg2.extras import DictCursor
from clickhouse_driver import Client as CHClient

from airflow import DAG
from airflow.operators.python import PythonOperator

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "bank_src")
PG_USER = os.getenv("PG_USER", "bank")
PG_PASS = os.getenv("PG_PASS", "bank")

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "airflow")
CH_PASS = os.getenv("CH_PASS", "airflow")


def pg_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode="require",
    )


def ch_client():
    return CHClient(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database="bank_dwh",
    )



def load_customer_dim():
    pg = pg_conn()
    ch = ch_client()

    with pg.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
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
                is_deleted::int AS is_deleted
            FROM bank.customer;
        """)
        rows = cur.fetchall()

    ch.execute("TRUNCATE TABLE bank_dwh.stg_customer;")
    if rows:
        data = [
            (
                r["customer_id"],
                r["customer_uuid"],
                r["full_name"],
                r["birth_date"],
                r["email"],
                r["phone"],
                r["city"],
                r["created_at"],
                r["updated_at"],
                r["is_deleted"],
            )
            for r in rows
        ]
        ch.execute("""
            INSERT INTO bank_dwh.stg_customer (
                customer_id, customer_uuid, full_name, birth_date,
                email, phone, city, created_at, updated_at, is_deleted
            ) VALUES
        """, data)

    ch.execute("TRUNCATE TABLE bank_dwh.raw_customer;")
    ch.execute("""
        INSERT INTO bank_dwh.raw_customer
        SELECT
            customer_id, customer_uuid, full_name, birth_date,
            email, phone, city, created_at, updated_at, is_deleted,
            now() AS loaded_at
        FROM bank_dwh.stg_customer;
    """)

    ch.execute("TRUNCATE TABLE bank_dwh.ods_dim_customer;")
    ch.execute("""
        INSERT INTO bank_dwh.ods_dim_customer
        SELECT
            customer_id, customer_uuid, full_name, birth_date,
            email, phone, city, is_deleted, now() AS loaded_at
        FROM bank_dwh.raw_customer;
    """)

    pg.close()


def load_account_dim():
    pg = pg_conn()
    ch = ch_client()

    with pg.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
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
                is_deleted::int AS is_deleted
            FROM bank.account;
        """)
        rows = cur.fetchall()

    ch.execute("TRUNCATE TABLE bank_dwh.stg_account;")
    if rows:
        data = [
            (
                r["account_id"],
                r["account_number"],
                r["customer_id"],
                r["currency_code"],
                r["opened_at"],
                r["closed_at"],
                r["status"],
                float(r["daily_transfer_limit"]),
                r["created_at"],
                r["updated_at"],
                r["is_deleted"],
            )
            for r in rows
        ]
        ch.execute("""
            INSERT INTO bank_dwh.stg_account (
                account_id, account_number, customer_id,
                currency_code, opened_at, closed_at, status,
                daily_transfer_limit, created_at, updated_at, is_deleted
            ) VALUES
        """, data)

    ch.execute("TRUNCATE TABLE bank_dwh.raw_account;")
    ch.execute("""
        INSERT INTO bank_dwh.raw_account
        SELECT
            account_id, account_number, customer_id,
            currency_code, opened_at, closed_at, status,
            daily_transfer_limit, created_at, updated_at,
            is_deleted, now() AS loaded_at
        FROM bank_dwh.stg_account;
    """)

    ch.execute("TRUNCATE TABLE bank_dwh.ods_dim_account;")
    ch.execute("""
        INSERT INTO bank_dwh.ods_dim_account
        SELECT
            account_id, account_number, customer_id,
            currency_code, opened_at, closed_at, status,
            daily_transfer_limit, is_deleted, now() AS loaded_at
        FROM bank_dwh.raw_account;
    """)

    pg.close()


def load_card_dim():
    pg = pg_conn()
    ch = ch_client()

    with pg.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT
                card_id,
                card_number,
                account_id,
                status,
                opened_at,
                closed_at,
                created_at,
                updated_at,
                is_deleted::int AS is_deleted
            FROM bank.card;
        """)
        rows = cur.fetchall()

    ch.execute("TRUNCATE TABLE bank_dwh.stg_card;")
    if rows:
        data = [
            (
                r["card_id"],
                r["card_number"],
                r["account_id"],
                r["status"],
                r["opened_at"],
                r["closed_at"],
                r["created_at"],
                r["updated_at"],
                r["is_deleted"],
            )
            for r in rows
        ]
        ch.execute("""
            INSERT INTO bank_dwh.stg_card (
                card_id, card_number, account_id,
                status, opened_at, closed_at,
                created_at, updated_at, is_deleted
            ) VALUES
        """, data)

    ch.execute("TRUNCATE TABLE bank_dwh.raw_card;")
    ch.execute("""
        INSERT INTO bank_dwh.raw_card
        SELECT
            card_id, card_number, account_id,
            status, opened_at, closed_at,
            created_at, updated_at, is_deleted,
            now() AS loaded_at
        FROM bank_dwh.stg_card;
    """)

    ch.execute("TRUNCATE TABLE bank_dwh.ods_dim_card;")
    ch.execute("""
        INSERT INTO bank_dwh.ods_dim_card
        SELECT
            card_id, card_number, account_id,
            status, opened_at, closed_at, is_deleted,
            now() AS loaded_at
        FROM bank_dwh.raw_card;
    """)

    pg.close()


def load_terminal_dim():
    pg = pg_conn()
    ch = ch_client()

    with pg.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT
                terminal_id,
                terminal_code,
                city,
                country,
                latitude,
                longitude,
                created_at,
                updated_at,
                is_working::int AS is_working,
                is_deleted::int AS is_deleted
            FROM bank.terminal;
        """)
        rows = cur.fetchall()

    ch.execute("TRUNCATE TABLE bank_dwh.stg_terminal;")
    if rows:
        data = [
            (
                r["terminal_id"],
                r["terminal_code"],
                r["city"],
                r["country"],
                float(r["latitude"]) if r["latitude"] is not None else None,
                float(r["longitude"]) if r["longitude"] is not None else None,
                r["created_at"],
                r["updated_at"],
                r["is_working"],
                r["is_deleted"],
            )
            for r in rows
        ]
        ch.execute("""
            INSERT INTO bank_dwh.stg_terminal (
                terminal_id, terminal_code, city, country,
                latitude, longitude, created_at, updated_at,
                is_working, is_deleted
            ) VALUES
        """, data)

    ch.execute("TRUNCATE TABLE bank_dwh.raw_terminal;")
    ch.execute("""
        INSERT INTO bank_dwh.raw_terminal
        SELECT
            terminal_id, terminal_code, city, country,
            latitude, longitude,
            created_at, updated_at,
            is_working, is_deleted,
            now() AS loaded_at
        FROM bank_dwh.stg_terminal;
    """)

    ch.execute("TRUNCATE TABLE bank_dwh.ods_dim_terminal;")
    ch.execute("""
        INSERT INTO bank_dwh.ods_dim_terminal
        SELECT
            terminal_id, terminal_code, city, country,
            latitude, longitude, is_working, is_deleted,
            now() AS loaded_at
        FROM bank_dwh.raw_terminal;
    """)

    pg.close()


PROCESS_NAME_TXN = "pg_to_ch_transaction"


def get_last_txn_id(ch: CHClient) -> int:
    rows = ch.execute("""
        SELECT last_id
        FROM bank_dwh.ctl_pg_load
        WHERE process_name = %(p)s AND src_table = 'bank.transaction'
        ORDER BY updated_at DESC
        LIMIT 1
    """, {"p": PROCESS_NAME_TXN})
    if rows:
        return int(rows[0][0])
    return 0


def upsert_last_txn_id(ch: CHClient, last_id: int):
    ch.execute("""
        INSERT INTO bank_dwh.ctl_pg_load (process_name, src_table, last_id)
        VALUES (%(p)s, 'bank.transaction', %(id)s)
    """, {"p": PROCESS_NAME_TXN, "id": last_id})


def load_transactions_and_fact():
    pg = pg_conn()
    ch = ch_client()

    last_id = get_last_txn_id(ch)
    print("Last txn_id in ctl:", last_id)

    with pg.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT
                txn_id,
                card_id,
                terminal_id,
                txn_ts,
                amount,
                currency_code,
                txn_type::text AS txn_type,
                status::text     AS status
            FROM bank.transaction
            WHERE txn_id > %s
            ORDER BY txn_id;
        """, (last_id,))
        rows = cur.fetchall()

    if rows:
        data = [
            (
                int(r["txn_id"]),
                r["card_id"],
                r["terminal_id"],
                r["txn_ts"],
                float(r["amount"]),
                r["currency_code"],
                r["txn_type"],
                r["status"],
            )
            for r in rows
        ]
        ch.execute("""
            INSERT INTO bank_dwh.stg_transaction (
                txn_id, card_id, terminal_id,
                txn_ts, amount, currency_code,
                txn_type, status
            ) VALUES
        """, data)

        ch.execute("""
            INSERT INTO bank_dwh.raw_transaction
            SELECT
                txn_id,
                card_id,
                terminal_id,
                txn_ts,
                amount,
                currency_code,
                txn_type,
                status,
                now() AS loaded_at
            FROM bank_dwh.stg_transaction
            WHERE txn_id > %(last_id)s
        """, {"last_id": last_id})

        new_last_id = max(r["txn_id"] for r in rows)
        upsert_last_txn_id(ch, new_last_id)
        print("Updated ctl last_id to:", new_last_id)
    else:
        print("No new transactions")

    ch.execute("TRUNCATE TABLE bank_dwh.ods_fact_atm_transaction;")
    ch.execute("""
        INSERT INTO bank_dwh.ods_fact_atm_transaction (
            txn_id, txn_ts, amount, currency_code,
            card_id, card_number,
            account_id, account_number, customer_id, customer_city,
            terminal_id, terminal_city,
            txn_type, status
        )
        SELECT
            t.txn_id,
            t.txn_ts,
            toFloat64(t.amount)          AS amount,
            t.currency_code,
            c.card_id,
            c.card_number,
            a.account_id,
            a.account_number,
            cust.customer_id,
            cust.city                    AS customer_city,
            term.terminal_id,
            term.city                    AS terminal_city,
            t.txn_type,
            t.status
        FROM bank_dwh.raw_transaction t
        LEFT JOIN bank_dwh.raw_card c
            ON c.card_id = t.card_id
        LEFT JOIN bank_dwh.raw_account a
            ON a.account_id = c.account_id
        LEFT JOIN bank_dwh.raw_customer cust
            ON cust.customer_id = a.customer_id
        LEFT JOIN bank_dwh.raw_terminal term
            ON term.terminal_id = t.terminal_id;
    """)

    pg.close()



with DAG(
    dag_id="pg_to_ch_simple_dwh",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="*/15 * * * *",  # каждые 15 минут
    catchup=False,
    max_active_runs=1,
    tags=["bank", "clickhouse", "postgres", "dwh"],
) as dag:

    t_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_customer_dim,
    )

    t_account = PythonOperator(
        task_id="load_dim_account",
        python_callable=load_account_dim,
    )

    t_card = PythonOperator(
        task_id="load_dim_card",
        python_callable=load_card_dim,
    )

    t_terminal = PythonOperator(
        task_id="load_dim_terminal",
        python_callable=load_terminal_dim,
    )

    t_txn_fact = PythonOperator(
        task_id="load_transactions_and_fact",
        python_callable=load_transactions_and_fact,
    )

    [t_customer, t_account, t_card, t_terminal] >> t_txn_fact
