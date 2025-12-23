#!/usr/bin/env python3
import json
import os
import signal
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from kafka import KafkaConsumer
from clickhouse_driver import Client as CHClient


# =======================
# Env / Config
# =======================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICS = [t.strip() for t in os.getenv("CDC_TOPICS", "").split(",") if t.strip()]
GROUP_ID = os.getenv("CDC_CONSUMER_GROUP", "ch_cdc_v1")

BATCH_SIZE = int(os.getenv("CDC_BATCH_SIZE", "500"))
POLL_TIMEOUT_MS = int(os.getenv("CDC_POLL_TIMEOUT_MS", "1000"))

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "airflow")
CH_PASS = os.getenv("CH_PASS", "airflow")
CH_DB = os.getenv("CH_DB", "bank_dwh")

# Debezium op types
# r = snapshot read, c = create/insert, u = update, d = delete
ALLOWED_OPS_TXN = {"r", "c"}                  # transaction is append-only in our model
ALLOWED_OPS_CARD = {"r", "c", "u", "d"}       # card is dimension -> full CDC

running = True


def _stop(*_: Any) -> None:
    global running
    running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


def _ts_from_ms(ts_ms: Optional[int]) -> datetime:
    if ts_ms is None:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    except Exception:
        return datetime.now(tz=timezone.utc)


def _safe_payload(value: Any) -> Optional[Dict[str, Any]]:
    """
    Debezium message usually:
      {"schema": ..., "payload": {"before":..., "after":..., "op":..., "ts_ms":...}}
    But may be already the payload, or other structures (heartbeat).
    """
    if not isinstance(value, dict):
        return None
    payload = value.get("payload", value)
    if not isinstance(payload, dict):
        return None
    return payload


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj or {}, ensure_ascii=False)


def _topic_kind(topic: str) -> str:
    """
    Route by topic suffix. Expected:
      bankcdc.bank.transaction
      bankcdc.bank.card
    """
    if topic.endswith(".transaction"):
        return "transaction"
    if topic.endswith(".card"):
        return "card"
    return "other"


def main() -> None:
    assert TOPICS, "CDC_TOPICS is empty. Example: CDC_TOPICS=bankcdc.bank.transaction,bankcdc.bank.card"

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,  # allows loop to check `running`
    )

    ch = CHClient(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASS,
        database=CH_DB,
    )

    buf_txn: List[Tuple[int, int, str, int, str, datetime]] = []
    buf_card: List[Tuple[int, int, str, int, str, str, datetime]] = []

    print(f"[cdc-consumer] bootstrap={KAFKA_BOOTSTRAP} group={GROUP_ID}")
    print(f"[cdc-consumer] topics={TOPICS}")
    print(f"[cdc-consumer] clickhouse={CH_HOST}:{CH_PORT}/{CH_DB}")

    while running:
        msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=BATCH_SIZE)
        if not msg_pack:
            continue

        for tp, messages in msg_pack.items():
            for msg in messages:
                payload = _safe_payload(msg.value)
                if payload is None:
                    continue

                op = payload.get("op")  # 'r','c','u','d' or None
                if op is None:
                    # heartbeat / non-row message
                    continue

                ts_ms = payload.get("ts_ms")
                before = payload.get("before") or {}
                after = payload.get("after") or {}

                kind = _topic_kind(msg.topic)

                if kind == "transaction":
                    # Append-only: accept snapshot + inserts
                    if op not in ALLOWED_OPS_TXN:
                        continue

                    txn_id = after.get("txn_id") or before.get("txn_id")
                    if txn_id is None:
                        continue

                    # For snapshot ('r') and insert ('c') after should exist; fallback to before if needed
                    payload_after = after if after else before

                    buf_txn.append(
                        (
                            int(msg.partition),
                            int(msg.offset),
                            op,
                            int(txn_id),
                            _json_dumps(payload_after),
                            _ts_from_ms(ts_ms),
                        )
                    )

                elif kind == "card":
                    # Dimension: accept snapshot + full CDC
                    if op not in ALLOWED_OPS_CARD:
                        continue

                    card_id = after.get("card_id") or before.get("card_id")
                    if card_id is None:
                        continue

                    buf_card.append(
                        (
                            int(msg.partition),
                            int(msg.offset),
                            op,
                            int(card_id),
                            _json_dumps(before),
                            _json_dumps(after),
                            _ts_from_ms(ts_ms),
                        )
                    )

                else:
                    # ignore other topics
                    continue

        # Flush in one transaction-like batch: write -> commit offsets
        wrote_any = False

        if buf_txn:
            ch.execute(
                """
                INSERT INTO stg_cdc_transaction
                (kafka_partition,kafka_offset,op,txn_id,payload_after,src_ts)
                VALUES
                """,
                buf_txn,
            )
            wrote_any = True

        if buf_card:
            ch.execute(
                """
                INSERT INTO stg_cdc_card
                (kafka_partition,kafka_offset,op,card_id,payload_before,payload_after,src_ts)
                VALUES
                """,
                buf_card,
            )
            wrote_any = True

        if wrote_any:
            consumer.commit()
            print(f"[cdc-consumer] wrote txn={len(buf_txn)} card={len(buf_card)}; committed offsets")

        buf_txn.clear()
        buf_card.clear()

    # graceful shutdown flush (best-effort)
    try:
        if buf_txn:
            ch.execute(
                """
                INSERT INTO stg_cdc_transaction
                (kafka_partition,kafka_offset,op,txn_id,payload_after,src_ts)
                VALUES
                """,
                buf_txn,
            )
        if buf_card:
            ch.execute(
                """
                INSERT INTO stg_cdc_card
                (kafka_partition,kafka_offset,op,card_id,payload_before,payload_after,src_ts)
                VALUES
                """,
                buf_card,
            )
        if buf_txn or buf_card:
            consumer.commit()
            print(f"[cdc-consumer] final flush txn={len(buf_txn)} card={len(buf_card)}; committed offsets")
    except Exception as e:
        print(f"[cdc-consumer] final flush failed: {e}")

    consumer.close()
    print("[cdc-consumer] stopped")


if __name__ == "__main__":
    main()
