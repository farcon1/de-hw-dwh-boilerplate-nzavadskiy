import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="cdc_to_dwh",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdc", "dbt", "clickhouse"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_cdc_models",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select "
            "raw_transaction_cdc "
            "ods_dim_card_scd2 "
            "ods_fact_atm_transaction"
        ),
    )
