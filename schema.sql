-- Расширения (для uuid и т.п.)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS bank;

-- ==== Справочник клиентов ====
CREATE TABLE bank.customer (
    customer_id     SERIAL PRIMARY KEY,
    customer_uuid   UUID NOT NULL DEFAULT gen_random_uuid(),
    full_name       TEXT NOT NULL,
    birth_date      DATE,
    email           TEXT,
    phone           TEXT,
    city            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE
);

-- ==== Счета ====
CREATE TABLE bank.account (
    account_id          SERIAL PRIMARY KEY,
    account_number      VARCHAR(34) NOT NULL UNIQUE,
    customer_id         INT NOT NULL REFERENCES bank.customer(customer_id),
    currency_code       CHAR(3) NOT NULL,
    opened_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at           TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'active', -- active / closed / blocked
    daily_transfer_limit NUMERIC(18,2) NOT NULL DEFAULT 100000,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted          BOOLEAN NOT NULL DEFAULT FALSE
);

-- ==== Карты ====
CREATE TABLE bank.card (
    card_id         SERIAL PRIMARY KEY,
    card_number     VARCHAR(16) NOT NULL UNIQUE,
    account_id      INT NOT NULL REFERENCES bank.account(account_id),
    status          TEXT NOT NULL DEFAULT 'active', -- active / blocked / closed
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE
);

-- ==== Терминалы / банкоматы ====
CREATE TABLE bank.terminal (
    terminal_id     SERIAL PRIMARY KEY,
    terminal_code   TEXT NOT NULL UNIQUE,
    city            TEXT NOT NULL,
    country         TEXT NOT NULL,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    is_working      BOOLEAN NOT NULL DEFAULT True,
    is_deleted      BOOLEAN NOT NULL DEFAULT FALSE
);

-- ==== Типы и статусы транзакций ====
CREATE TYPE bank.txn_type AS ENUM (
    'atm_withdrawal',
    'atm_deposit'
);

CREATE TYPE bank.txn_status AS ENUM (
    'approved',
    'declined',
    'reversed'
);

-- ==== Транзакции ====
CREATE TABLE bank.transaction (
    txn_id          BIGSERIAL PRIMARY KEY,
    card_id         INT NOT NULL REFERENCES bank.card(card_id),
    terminal_id     INT REFERENCES bank.terminal(terminal_id),
    txn_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- время проведения операции
    amount          NUMERIC(18,2) NOT NULL,
    currency_code   CHAR(3) NOT NULL,
    txn_type        bank.txn_type NOT NULL,
    status          bank.txn_status NOT NULL -- approved/declined/reversed
);


-- -- ==== CTL-таблица для ETL ====
-- CREATE TABLE bank.ctl_load (
--     process_name    TEXT PRIMARY KEY,
--     last_success_ts TIMESTAMPTZ
-- );
