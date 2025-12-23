# README — bank DE demo (schema, filler, simple-etl)

Short guide how the repository pieces fit together and how to run the filler, the source DB and the simple ETL / DWH pipeline.

Overview
- Purpose: demo bank dataset, synthetic data generator (filler), CDC pipeline and simple DWH (ClickHouse + dbt + Airflow).
- Key artifacts:
    - schema.sql — DB schema for source Postgres (schema: bank.*).
    - new_events.py — Python filler that inserts customers, accounts, cards, terminals and many transactions (including fraud scenarios and backfill).
    - simple-etl/ — orchestrates CDC and DWH: docker-compose, Airflow DAGs, dbt project, ClickHouse init scripts, Debezium connectors and a CDC consumer.

Files & responsibilities
- /schema.sql
    - Creates schema bank, tables: customer, account, card, terminal, transaction.
    - Creates pgcrypto extension and two enum types: bank.txn_type, bank.txn_status.
    - Run this on your Postgres source before generating data.
- /new_events.py
    - Configurable behaviour via environment variables:
        - DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT (defaults in file).
    - Generates:
        - New customers/accounts/cards and terminals.
        - Current transactions (N_NEW_TXNS) and optional backfill (BACKFILL_DAYS).
        - Logical closes of cards/accounts/customers/terminals.
        - Fraud scenarios (rapid withdrawals, attempts after card closed, amount probing).
    - Connects to Postgres via psycopg2. By default DSN uses sslmode=verify-full — adjust if your local Postgres doesn't use SSL.
- /simple-etl/
    - docker-compose.yml — brings up the stack (Airflow, Debezium/Kafka connectors if configured, ClickHouse, etc. — review compose for enabled services).
    - airflow/ — Dockerfile, plugins and DAGs:
        - dags/pg_to_ch_dag.py — moves raw data from Postgres to ClickHouse (CDC based).
        - dags/cdc_monitoring.py, cdc_to_dwh.py, dbt_incremental_dag.py — helpers to run CDC and dbt transformations.
    - clickhouse_init/01_schema_dwh.sql — DWH schema for ClickHouse; run once into ClickHouse.
    - dbt/ — dbt project:
        - models/raw/, stg/, ods/ — definitions for raw and ODS layers and transformations.
        - profiles.yml / .user.yml — configure connection to ClickHouse (or target).
    - debezium/ — example Debezium connector JSONs for Postgres topics (postgres-card.json, postgres-transaction.json).

Quickstart (minimal local)
1. Prepare Postgres (source)
     - Create DB and user, or use existing Postgres.
     - Apply schema:
         psql -h <host> -p <port> -U <user> -d <db> -f schema.sql
     - Note: schema.sql requires permission to CREATE EXTENSION pgcrypto.

2. Configure environment for filler
     - Create .env (or export env vars) used by new_events.py. Example .env:
         DB_NAME=bankdb
         DB_USER=bank
         DB_PASSWORD=password
         DB_HOST=localhost
         DB_PORT=5432
     - Or set environment directly before running.

3. Run data filler
     - Install requirements (psycopg2, python-dotenv) in a virtualenv.
         pip install psycopg2-binary python-dotenv
     - Run:
         python new_events.py
     - The script will insert new customers, accounts, cards, terminals and transactions (including fraud patterns). Edit constants at top of file (N_NEW_TXNS, BACKFILL_DAYS, FRAUD_SHARE) to change volume/behavior.

4. Start simple-etl stack (optional, to build DWH)
     - From simple-etl/ directory:
         docker-compose up -d
     - Wait for services (Airflow, ClickHouse, Kafka/Debezium if present) to be healthy.

5. Initialize ClickHouse DWH schema
     - Run clickhouse_init/01_schema_dwh.sql against your ClickHouse instance (check docker-compose for hostname/port).

6. CDC / Debezium (optional)
     - If Debezium / Kafka Connect is available in your stack:
         - Register connector configs in debezium/*.json via Debezium REST API.
         - Debezium will stream Postgres table changes to Kafka topics consumed by the CDC consumer / Airflow DAGs in simple-etl.

7. Airflow / DAGs / dbt
     - Airflow UI exposes DAGs in simple-etl/airflow/dags:
         - pg_to_ch_dag.py and cdc_to_dwh.py move CDC/raw data to ClickHouse.
         - dbt_incremental_dag.py triggers dbt runs for transformations.
     - dbt:
         - Configure dbt profiles (dbt/profiles.yml) to point to your ClickHouse target.
         - Run locally or via Airflow:
             cd dbt
             dbt run           # run models (or dbt run --models ods.*)
             dbt test

Recommended sequence for demo
- Apply schema.sql to Postgres.
- docker-compose up -d (simple-etl) to start DWH infra.
- Run new_events.py several times to populate source with transactions.
- Register Debezium connectors (if used) and start CDC DAGs in Airflow.
- Initialize ClickHouse schema, then run dbt transformations to populate ODS/fact tables.

Troubleshooting tips
- DSN SSL: new_events.py uses sslmode=verify-full. If your dev Postgres doesn't use SSL, edit DSN string to remove ssl options or set sslmode=disable.
- Extensions: CREATE EXTENSION pgcrypto requires superuser rights.
- If Airflow DAGs don't pick up, check logs in simple-etl/airflow/logs scheduler and worker containers.
- For high-volume backfill, lower BACKFILL_TXNS_PER_DAY or run in batches.

Where to look next
- Examine dbt models: dbt/models/{raw,stg,ods} to see how raw Postgres -> ods tables are modeled.
- Look at simple-etl/airflow/dags for orchestration logic and scheduling.
- Debezium JSON files show which tables are captured for CDC.

Contact
- This README is a concise operational guide. For changes to data volumes or behavior, edit top-level constants in new_events.py and re-run.
