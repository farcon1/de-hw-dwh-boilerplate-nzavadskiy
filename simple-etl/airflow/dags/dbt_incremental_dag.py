import os
from datetime import datetime

import pendulum
import psycopg2
from psycopg2.extras import DictCursor
from clickhouse_driver import Client as CHClient

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "bank_src")
PG_USER = os.getenv("PG_USER", "bank")

PG_PASS = os.getenv("PG_PASS") or os.getenv("PG_PASSWORD") or "bank"

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
        connect_timeout=10,
    )


def ch_client():
    return CHClient(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database="bank_dwh",
    )




def load_stg_from_pg():
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
        ch.execute("""
            INSERT INTO bank_dwh.stg_customer (
                customer_id, customer_uuid, full_name, birth_date,
                email, phone, city, created_at, updated_at, is_deleted
            ) VALUES
        """, [
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
        ])

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
        ch.execute("""
            INSERT INTO bank_dwh.stg_account (
                account_id, account_number, customer_id,
                currency_code, opened_at, closed_at, status,
                daily_transfer_limit, created_at, updated_at, is_deleted
            ) VALUES
        """, [
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
        ])

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
        ch.execute("""
            INSERT INTO bank_dwh.stg_card (
                card_id, card_number, account_id,
                status, opened_at, closed_at,
                created_at, updated_at, is_deleted
            ) VALUES
        """, [
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
        ])

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
        ch.execute("""
            INSERT INTO bank_dwh.stg_terminal (
                terminal_id, terminal_code, city, country,
                latitude, longitude,
                created_at, updated_at,
                is_working, is_deleted
            ) VALUES
        """, [
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
        ])

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
                status::text   AS status
            FROM bank.transaction;
        """)
        rows = cur.fetchall()

    ch.execute("TRUNCATE TABLE bank_dwh.stg_transaction;")
    if rows:
        ch.execute("""
            INSERT INTO bank_dwh.stg_transaction (
                txn_id, card_id, terminal_id,
                txn_ts, amount, currency_code,
                txn_type, status
            ) VALUES
        """, [
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
        ])

    pg.close()


with DAG(
    dag_id="pg_to_ch_dbt_incremental_dwh",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="*/20 * * * *",  # каждые 20 минут
    catchup=False,
    max_active_runs=1,
    tags=["bank", "clickhouse", "postgres", "dbt", "incremental"],
) as dag:

    load_stg = PythonOperator(
        task_id="load_stg_from_pg",
        python_callable=load_stg_from_pg,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_raw_ods",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt ls && "
            "dbt run --select "
            "path:models/raw "
            "path:models/ods "
            "path:models/cdm "
            "path:models/dm"
        ),
    )

    load_stg >> dbt_run
