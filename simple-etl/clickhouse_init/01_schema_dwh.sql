CREATE DATABASE IF NOT EXISTS bank_dwh;

USE bank_dwh;

-- =========================
-- CTL для загрузки из Postgres
-- =========================
CREATE TABLE IF NOT EXISTS ctl_pg_load (
    process_name String,
    src_table    String,
    last_id      UInt64,
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (process_name, src_table);

-- =========================
-- STG таблицы (почти 1:1 с PG + extracted_at)
-- =========================

CREATE TABLE IF NOT EXISTS stg_customer (
    customer_id   Int32,
    customer_uuid UUID,
    full_name     String,
    birth_date    Date,
    email         String,
    phone         String,
    city          String,
    created_at    DateTime,
    updated_at    DateTime,
    is_deleted    UInt8,
    extracted_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (customer_id, extracted_at);

CREATE TABLE IF NOT EXISTS stg_account (
    account_id           Int32,
    account_number       String,
    customer_id          Int32,
    currency_code        FixedString(3),
    opened_at            DateTime,
    closed_at            Nullable(DateTime),
    status               String,
    daily_transfer_limit Decimal(18,2),
    created_at           DateTime,
    updated_at           DateTime,
    is_deleted           UInt8,
    extracted_at         DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (account_id, extracted_at);

CREATE TABLE IF NOT EXISTS stg_card (
    card_id     Int32,
    card_number String,
    account_id  Int32,
    status      String,
    opened_at   DateTime,
    closed_at   Nullable(DateTime),
    created_at  DateTime,
    updated_at  DateTime,
    is_deleted  UInt8,
    extracted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (card_id, extracted_at);

CREATE TABLE IF NOT EXISTS stg_terminal (
    terminal_id   Int32,
    terminal_code String,
    city          String,
    country       String,
    latitude      Float64,
    longitude     Float64,
    created_at    DateTime,
    updated_at    DateTime,
    is_working    UInt8,
    is_deleted    UInt8,
    extracted_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (terminal_id, extracted_at);

CREATE TABLE IF NOT EXISTS stg_transaction (
    txn_id        UInt64,
    card_id       Int32,
    terminal_id   Int32,
    txn_ts        DateTime,
    amount        Decimal(18,2),
    currency_code FixedString(3),
    txn_type      String,
    status        String,
    extracted_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (txn_id, extracted_at);

-- =========================
-- RAW слой (простая копия источника + loaded_at)
-- =========================

CREATE TABLE IF NOT EXISTS raw_customer (
    customer_id   Int32,
    customer_uuid UUID,
    full_name     String,
    birth_date    Date,
    email         String,
    phone         String,
    city          String,
    created_at    DateTime,
    updated_at    DateTime,
    is_deleted    UInt8,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS raw_account (
    account_id           Int32,
    account_number       String,
    customer_id          Int32,
    currency_code        FixedString(3),
    opened_at            DateTime,
    closed_at            Nullable(DateTime),
    status               String,
    daily_transfer_limit Decimal(18,2),
    created_at           DateTime,
    updated_at           DateTime,
    is_deleted           UInt8,
    loaded_at            DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS raw_card (
    card_id     Int32,
    card_number String,
    account_id  Int32,
    status      String,
    opened_at   DateTime,
    closed_at   Nullable(DateTime),
    created_at  DateTime,
    updated_at  DateTime,
    is_deleted  UInt8,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY card_id;

CREATE TABLE IF NOT EXISTS raw_terminal (
    terminal_id   Int32,
    terminal_code String,
    city          String,
    country       String,
    latitude      Float64,
    longitude     Float64,
    created_at    DateTime,
    updated_at    DateTime,
    is_working    UInt8,
    is_deleted    UInt8,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY terminal_id;

CREATE TABLE IF NOT EXISTS raw_transaction (
    txn_id        UInt64,
    card_id       Int32,
    terminal_id   Int32,
    txn_ts        DateTime,
    amount        Decimal(18,2),
    currency_code FixedString(3),
    txn_type      String,
    status        String,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(txn_ts)
ORDER BY txn_id;

-- =========================
-- ODS слой: DIM и FACT
-- =========================

CREATE TABLE IF NOT EXISTS ods_dim_customer (
    customer_id   Int32,
    customer_uuid UUID,
    full_name     String,
    birth_date    Date,
    email         String,
    phone         String,
    city          String,
    is_deleted    UInt8,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS ods_dim_account (
    account_id           Int32,
    account_number       String,
    customer_id          Int32,
    currency_code        FixedString(3),
    opened_at            DateTime,
    closed_at            Nullable(DateTime),
    status               String,
    daily_transfer_limit Decimal(18,2),
    is_deleted           UInt8,
    loaded_at            DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS ods_dim_card (
    card_id     Int32,
    card_number String,
    account_id  Int32,
    status      String,
    opened_at   DateTime,
    closed_at   Nullable(DateTime),
    is_deleted  UInt8,
    loaded_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY card_id;

CREATE TABLE IF NOT EXISTS ods_dim_terminal (
    terminal_id   Int32,
    terminal_code String,
    city          String,
    country       String,
    latitude      Float64,
    longitude     Float64,
    is_working    UInt8,
    is_deleted    UInt8,
    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY terminal_id;

CREATE TABLE IF NOT EXISTS ods_fact_atm_transaction (
    txn_id        UInt64,
    txn_ts        DateTime,
    amount        Float64,
    currency_code FixedString(3),

    card_id       Int32,
    card_number   String,

    account_id    Int32,
    account_number String,
    customer_id   Int32,
    customer_city String,

    terminal_id   Int32,
    terminal_city String,

    txn_type      String,
    status        String,

    loaded_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(txn_ts)
ORDER BY (txn_ts, txn_id);


CREATE TABLE IF NOT EXISTS bank_dwh.ctl_monitoring (
  ts DateTime DEFAULT now(),
  metric String,
  value Float64,
  labels String
)
ENGINE = MergeTree
ORDER BY (ts, metric);

CREATE TABLE IF NOT EXISTS bank_dwh.stg_cdc_card (
    kafka_partition Int32,
    kafka_offset    Int64,
    op              Enum8('c'=1,'u'=2,'d'=3),
    card_id         Int32,
    payload_before  String,
    payload_after   String,
    src_ts          DateTime64(3),
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (kafka_partition, kafka_offset);

CREATE TABLE IF NOT EXISTS bank_dwh.ods_dim_card_scd2 (
    card_sk     UInt64,
    card_id     Int32,
    card_number String,
    account_id  Int32,
    status      String,
    opened_at   DateTime,
    closed_at   Nullable(DateTime),
    is_deleted  UInt8,

    valid_from  DateTime64(3),
    valid_to    DateTime64(3),
    is_current  UInt8,

    src_ts      DateTime64(3),
    loaded_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (card_id, valid_from);


CREATE TABLE IF NOT EXISTS bank_dwh.stg_cdc_transaction (
    kafka_partition Int32,
    kafka_offset    Int64,
    op              Enum8('c'=1,'u'=2,'d'=3),
    txn_id          UInt64,
    payload_after   String,
    src_ts          DateTime64(3),
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (kafka_partition, kafka_offset);

CREATE TABLE IF NOT EXISTS bank_dwh.ctl_cdc_offsets (
  topic String,
  partition Int32,
  last_offset Int64,
  updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (topic, partition);
