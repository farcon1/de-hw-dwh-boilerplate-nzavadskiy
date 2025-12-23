import os, json, requests
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from clickhouse_driver import Client as CHClient


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.getenv("CDC_CONSUMER_GROUP", "ch_cdc_v1")
TOPICS = [t.strip() for t in os.getenv("CDC_TOPICS", "").split(",") if t.strip()]

DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")
DEBEZIUM_CONNECTORS = [c.strip() for c in os.getenv("DEBEZIUM_CONNECTORS", "bank-transaction-cdc,bank-card-cdc").split(",") if c.strip()]

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "airflow")
CH_PASS = os.getenv("CH_PASS", "airflow")

LAG_THRESHOLD = int(os.getenv("CDC_LAG_THRESHOLD", "5000"))


def ch():
    return CHClient(host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database="bank_dwh")


def check_debezium():
    c = ch()
    for name in DEBEZIUM_CONNECTORS:
        r = requests.get(f"{DEBEZIUM_URL}/connectors/{name}/status", timeout=5)
        ok = 1.0 if r.status_code == 200 and r.json().get("connector", {}).get("state") == "RUNNING" else 0.0
        c.execute("INSERT INTO ctl_monitoring(metric,value,labels) VALUES", [
            ("debezium_connector_running", ok, json.dumps({"connector": name}))
        ])
        if ok < 1.0:
            raise RuntimeError(f"Debezium connector not RUNNING: {name} / {r.text}")


def check_kafka_lag():
    consumer = KafkaConsumer(bootstrap_servers=[KAFKA_BOOTSTRAP], enable_auto_commit=False, group_id=GROUP_ID)
    c = ch()

    total_lag = 0
    for topic in TOPICS:
        parts = consumer.partitions_for_topic(topic) or set()
        for p in parts:
            tp = TopicPartition(topic, p)
            consumer.assign([tp])
            committed = consumer.committed(tp) or 0
            end_offset = consumer.end_offsets([tp])[tp]
            lag = max(0, end_offset - committed)
            total_lag += lag
            c.execute("INSERT INTO ctl_monitoring(metric,value,labels) VALUES", [
                ("kafka_partition_lag", float(lag), json.dumps({"topic": topic, "partition": p}))
            ])

    c.execute("INSERT INTO ctl_monitoring(metric,value,labels) VALUES", [
        ("kafka_total_lag", float(total_lag), json.dumps({"group": GROUP_ID}))
    ])

    if total_lag > LAG_THRESHOLD:
        raise RuntimeError(f"CDC lag too high: {total_lag} > {LAG_THRESHOLD}")


with DAG(
    dag_id="cdc_monitoring",
    start_date=pendulum.datetime(2025,1,1,tz="UTC"),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["cdc","monitoring"],
) as dag:
    t1 = PythonOperator(task_id="check_debezium", python_callable=check_debezium)
    t2 = PythonOperator(task_id="check_kafka_lag", python_callable=check_kafka_lag)
    t1 >> t2
