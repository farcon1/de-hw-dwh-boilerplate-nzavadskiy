import random
import os
from datetime import datetime, timedelta, timezone

import psycopg2

import dotenv


# ===== КОНФИГУРАЦИЯ =====

dotenv.load_dotenv()

DB = os.getenv("DB_NAME", "db")
DB_USER = os.getenv("DB_USER", "bank")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "6432")

DSN = f"dbname={DB} user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} sslmode=verify-full target_session_attrs=read-write"

# === Объёмы изменений за один запуск ===
N_NEW_CUSTOMERS = 5
N_NEW_TERMINALS = 2
N_NEW_TXNS = 200              # текущие транзакции (за последний час)
N_CLOSE_CARDS = 3
N_CLOSE_ACCOUNTS = 2
N_CLOSE_CUSTOMERS = 1
N_CLOSE_TERMINALS = 1

# === Фрод ===
FRAUD_SHARE = 0.15            # доля вызовов фрод-сценариев среди генераций транзакций

# === Бэкфилл ===
BACKFILL_DAYS = 30              # сколько дней назад делать бэкфилл
BACKFILL_TXNS_PER_DAY = 1000    # сколько транзакций на день


def get_conn():
    return psycopg2.connect(DSN)


# ===== ВСПОМОГАТЕЛЬНЫЕ =====

def fetch_all(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or {})
        return cur.fetchall()


def fetch_one(conn, query, params=None):
    with conn.cursor() as cur:
        cur.execute(query, params or {})
        return cur.fetchone()


def random_city():
    return random.choice(["Moscow", "SPB", "Berlin", "Prague", "NYC"])


# ===== СОЗДАНИЕ СУЩНОСТЕЙ =====

def create_customer_with_account_and_card(conn):
    full_name = f"Customer {random.randint(100000, 999999)}"
    birth_date = datetime(1980, 1, 1).date() + timedelta(days=random.randint(0, 15000))
    email = f"{full_name.replace(' ', '').lower()}@example.com"
    phone = f"+1000000{random.randint(1000, 9999)}"
    city = random_city()

    with conn.cursor() as cur:
        # customer
        cur.execute(
            """
            INSERT INTO bank.customer (full_name, birth_date, email, phone, city)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING customer_id;
            """,
            (full_name, birth_date, email, phone, city),
        )
        customer_id = cur.fetchone()[0]

        # account
        account_number = f"ACC{random.randint(10**9, 10**10-1)}"
        currency_code = random.choice(["RUB", "USD", "EUR"])
        daily_limit = random.choice([50_000, 100_000, 200_000])

        cur.execute(
            """
            INSERT INTO bank.account (account_number, customer_id, currency_code, daily_transfer_limit)
            VALUES (%s, %s, %s, %s)
            RETURNING account_id;
            """,
            (account_number, customer_id, currency_code, daily_limit),
        )
        account_id = cur.fetchone()[0]

        # card
        card_number = str(random.randint(4_000_000_000_000_000, 4_999_999_999_999_999))
        cur.execute(
            """
            INSERT INTO bank.card (card_number, account_id)
            VALUES (%s, %s)
            RETURNING card_id;
            """,
            (card_number, account_id),
        )
        card_id = cur.fetchone()[0]

    conn.commit()
    print(f"[NEW] customer={customer_id}, account={account_id}, card={card_id}")
    return customer_id, account_id, card_id


def create_terminal(conn):
    terminal_code = f"ATM-{random.randint(10000, 99999)}"
    city = random_city()
    country = random.choice(["RU", "DE", "CZ", "US"])
    lat = 55 + random.random()
    lon = 37 + random.random()

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bank.terminal (terminal_code, city, country, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING terminal_id;
            """,
            (terminal_code, city, country, lat, lon),
        )
        terminal_id = cur.fetchone()[0]
    conn.commit()
    print(f"[NEW] terminal={terminal_id} code={terminal_code}")
    return terminal_id


# ===== ЛОГИЧЕСКОЕ ЗАКРЫТИЕ =====

def close_random_card(conn):
    row = fetch_one(
        conn,
        """
        SELECT c.card_id
        FROM bank.card c
        JOIN bank.account a ON c.account_id = a.account_id
        JOIN bank.customer cu ON a.customer_id = cu.customer_id
        WHERE c.status = 'active'
          AND coalesce(c.is_deleted, FALSE) = FALSE
          AND a.status = 'active'
          AND coalesce(a.is_deleted, FALSE) = FALSE
          AND coalesce(cu.is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not row:
        return None
    card_id = row[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bank.card
            SET status = 'closed',
                is_deleted = TRUE,
                closed_at = now(),
                updated_at = now()
            WHERE card_id = %s;
            """,
            (card_id,),
        )
    conn.commit()
    print(f"[CLOSE] card={card_id}")
    return card_id


def close_random_account(conn):
    """
    Закрываем аккаунт: помечаем его и все его активные карты закрытыми.
    """
    row = fetch_one(
        conn,
        """
        SELECT a.account_id
        FROM bank.account a
        JOIN bank.customer cu ON a.customer_id = cu.customer_id
        WHERE a.status = 'active'
          AND coalesce(a.is_deleted, FALSE) = FALSE
          AND coalesce(cu.is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not row:
        return None
    account_id = row[0]
    with conn.cursor() as cur:
        # закрываем аккаунт
        cur.execute(
            """
            UPDATE bank.account
            SET status = 'closed',
                is_deleted = TRUE,
                closed_at = now(),
                updated_at = now()
            WHERE account_id = %s;
            """,
            (account_id,),
        )
        # закрываем связанные активные карты
        cur.execute(
            """
            UPDATE bank.card
            SET status = 'closed',
                is_deleted = TRUE,
                closed_at = now(),
                updated_at = now()
            WHERE account_id = %s
              AND status = 'active'
              AND coalesce(is_deleted, FALSE) = FALSE;
            """,
            (account_id,),
        )
    conn.commit()
    print(f"[CLOSE] account={account_id} (and its active cards)")
    return account_id


def close_random_customer(conn):
    """
    Логически "закрываем" клиента, но только если у него уже нет активных аккаунтов.
    """
    row = fetch_one(
        conn,
        """
        SELECT cu.customer_id
        FROM bank.customer cu
        LEFT JOIN bank.account a
          ON a.customer_id = cu.customer_id
         AND a.status = 'active'
         AND coalesce(a.is_deleted, FALSE) = FALSE
        WHERE coalesce(cu.is_deleted, FALSE) = FALSE
        GROUP BY cu.customer_id
        HAVING COUNT(a.account_id) = 0
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not row:
        return None
    customer_id = row[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bank.customer
            SET is_deleted = TRUE,
                updated_at = now()
            WHERE customer_id = %s;
            """,
            (customer_id,),
        )
    conn.commit()
    print(f"[CLOSE] customer={customer_id}")
    return customer_id


def close_random_terminal(conn):
    row = fetch_one(
        conn,
        """
        SELECT terminal_id
        FROM bank.terminal
        WHERE is_working = TRUE
          AND coalesce(is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not row:
        return None
    terminal_id = row[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bank.terminal
            SET is_working = FALSE,
                is_deleted = TRUE,
                updated_at = now()
            WHERE terminal_id = %s;
            """,
            (terminal_id,),
        )
    conn.commit()
    print(f"[CLOSE] terminal={terminal_id}")
    return terminal_id


# ===== ТРАНЗАКЦИИ: БАЗОВОЕ ГЕНЕРАЦИЯ =====

def random_amount():
    return round(random.uniform(100, 50_000), 2)


def random_currency():
    return random.choice(["RUB", "USD", "EUR"])


def random_txn_type():
    return random.choice(["atm_withdrawal", "atm_deposit"])


def random_status():
    r = random.random()
    if r < 0.92:
        return "approved"
    elif r < 0.97:
        return "declined"
    else:
        return "reversed"


def get_random_active_card_and_terminal(conn):
    card_row = fetch_one(
        conn,
        """
        SELECT c.card_id
        FROM bank.card c
        JOIN bank.account a ON c.account_id = a.account_id
        JOIN bank.customer cu ON a.customer_id = cu.customer_id
        WHERE c.status = 'active'
          AND coalesce(c.is_deleted, FALSE) = FALSE
          AND a.status = 'active'
          AND coalesce(a.is_deleted, FALSE) = FALSE
          AND coalesce(cu.is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not card_row:
        return None, None

    term_row = fetch_one(
        conn,
        """
        SELECT terminal_id
        FROM bank.terminal
        WHERE is_working = TRUE
          AND coalesce(is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not term_row:
        return card_row[0], None

    return card_row[0], term_row[0]


def insert_raw_txn(conn, card_id, terminal_id, txn_ts, amount, currency, txn_type, status):
    if card_id is None or terminal_id is None:
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bank.transaction
                (card_id, terminal_id, txn_ts, amount, currency_code, txn_type, status)
            VALUES
                (%s, %s, %s, %s, %s, %s::bank.txn_type, %s::bank.txn_status);
            """,
            (card_id, terminal_id, txn_ts, amount, currency, txn_type, status),
        )
    conn.commit()


def insert_normal_txn_current(conn):
    """
    Обычная транзакция за последний час.
    """
    card_id, term_id = get_random_active_card_and_terminal(conn)
    if card_id is None or term_id is None:
        return
    txn_ts = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 60))
    amount = random_amount()
    currency = random_currency()
    txn_type = random_txn_type()
    status = random_status()
    insert_raw_txn(conn, card_id, term_id, txn_ts, amount, currency, txn_type, status)


def insert_normal_txn_on_date(conn, date_for_day):
    """
    Обычная транзакция на конкретный день (бэкфилл).
    date_for_day — date (без времени).
    """
    card_id, term_id = get_random_active_card_and_terminal(conn)
    if card_id is None or term_id is None:
        return
    # случайное время в рамках дня
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    txn_ts = datetime(
        year=date_for_day.year,
        month=date_for_day.month,
        day=date_for_day.day,
        hour=hour,
        minute=minute,
        second=second,
        tzinfo=timezone.utc,
    )
    amount = random_amount()
    currency = random_currency()
    txn_type = random_txn_type()
    status = random_status()
    insert_raw_txn(conn, card_id, term_id, txn_ts, amount, currency, txn_type, status)


# ===== ФРОДОВЫЕ СЦЕНАРИИ =====

def get_random_active_card(conn):
    row = fetch_one(
        conn,
        """
        SELECT c.card_id
        FROM bank.card c
        JOIN bank.account a ON c.account_id = a.account_id
        JOIN bank.customer cu ON a.customer_id = cu.customer_id
        WHERE c.status = 'active'
          AND coalesce(c.is_deleted, FALSE) = FALSE
          AND a.status = 'active'
          AND coalesce(a.is_deleted, FALSE) = FALSE
          AND coalesce(cu.is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    return None if not row else row[0]


def get_random_working_terminals(conn, limit=10):
    rows = fetch_all(
        conn,
        """
        SELECT terminal_id
        FROM bank.terminal
        WHERE is_working = TRUE
          AND coalesce(is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT %s;
        """,
        (limit,),
    )
    return [r[0] for r in rows]


def fraud_rapid_withdrawals_diff_terminals(conn):
    """
    Сценарий 1: серия быстрых снятий в разных терминалах.
    """
    card_id = get_random_active_card(conn)
    if not card_id:
        return
    terminals = get_random_working_terminals(conn, limit=6)
    if not terminals:
        return

    base_ts = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 10))
    n_ops = random.randint(3, 6)
    print(f"[FRAUD] rapid_withdrawals card={card_id}, ops={n_ops}")

    for i in range(n_ops):
        terminal_id = random.choice(terminals)
        txn_ts = base_ts + timedelta(minutes=i * random.randint(1, 3))
        amount = round(random.uniform(5_000, 40_000), 2)
        currency = random.choice(["RUB", "EUR"])
        txn_type = "atm_withdrawal"
        status = "approved" if i < n_ops - 1 else random.choice(["declined", "reversed"])

        insert_raw_txn(conn, card_id, terminal_id, txn_ts, amount, currency, txn_type, status)


def fraud_after_card_closed(conn):
    """
    Сценарий 2: карта блокируется, а затем появляются попытки операций (declined).
    """
    # возьмём любую активную карту и заблокируем её
    row = fetch_one(
        conn,
        """
        SELECT card_id
        FROM bank.card
        WHERE status = 'active'
          AND coalesce(is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not row:
        return
    card_id = row[0]

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE bank.card
            SET status = 'blocked',
                updated_at = now()
            WHERE card_id = %s;
            """,
            (card_id,),
        )
    conn.commit()

    print(f"[FRAUD] card_closed_then_used card={card_id}")

    term_row = fetch_one(
        conn,
        """
        SELECT terminal_id
        FROM bank.terminal
        WHERE is_working = TRUE
          AND coalesce(is_deleted, FALSE) = FALSE
        ORDER BY random()
        LIMIT 1;
        """,
    )
    if not term_row:
        return
    terminal_id = term_row[0]

    base_ts = datetime.now(timezone.utc) + timedelta(minutes=1)
    for i in range(random.randint(1, 3)):
        txn_ts = base_ts + timedelta(minutes=i)
        amount = round(random.uniform(1_000, 10_000), 2)
        currency = random_currency()
        txn_type = "atm_withdrawal"
        status = "declined"

        insert_raw_txn(conn, card_id, terminal_id, txn_ts, amount, currency, txn_type, status)


def generate_amount_probe_fraud(
    cur,
    card_id: int,
    terminal_id: int,
    base_ts: datetime | None = None,
    currency_code: str = "RUB",
    txn_type: str = "atm_withdrawal",
    attempts_min: int = 3,
    attempts_max: int = 5,
    start_amount_min: int = 15000,
    start_amount_max: int = 90000,
    step_min: int = 500,
    step_max: int = 7000,
    inter_attempt_sec_min: int = 20,
    inter_attempt_sec_max: int = 120,
    final_success_prob: float = 0.45,
):
    """
    Моделирует fraud pattern: серия попыток снять наличные с уменьшающейся суммой.
    Транзакции неизменяемы -> только INSERT.
    """
    if base_ts is None:
        base_ts = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 60))

    attempts = random.randint(attempts_min, attempts_max)

    # стартовая сумма
    amount = random.randint(start_amount_min, start_amount_max)

    rows = []
    ts = base_ts

    for i in range(attempts):
        # уменьшаем сумму начиная со 2-й попытки
        if i > 0:
            dec = random.randint(step_min, step_max)
            amount = max(500, amount - dec)  # защитный минимум

        # статусы: первые почти всегда declined, последняя может быть успешной
        if i < attempts - 1:
            status = "declined"
        else:
            status = "approved" if random.random() < final_success_prob else "declined"

        rows.append((card_id, terminal_id, ts, float(amount), currency_code, txn_type, status))

        # следующий timestamp
        ts += timedelta(seconds=random.randint(inter_attempt_sec_min, inter_attempt_sec_max))

    cur.executemany(
        """
        INSERT INTO bank.transaction (card_id, terminal_id, txn_ts, amount, currency_code, txn_type, status)
        VALUES (%s, %s, %s, %s, %s, %s::bank.txn_type, %s::bank.txn_status)
        """,
        rows,
    )

    return {
        "attempts": attempts,
        "first_ts": rows[0][2],
        "last_ts": rows[-1][2],
        "amounts": [r[3] for r in rows],
        "statuses": [r[6] for r in rows],
        "card_id": card_id,
        "terminal_id": terminal_id,
    }


def fraud_amount_probe(conn):
    with conn.cursor() as cur:
        # выбираем карту и терминал
        card_id = get_random_active_card(conn)
        if not card_id:
            return
        terminals = get_random_working_terminals(conn, limit=1)
        if not terminals:
            return
        terminal_id = terminals[0]

        # вставляем новый вид фрода
        info = generate_amount_probe_fraud(
            cur=cur,
            card_id=card_id,
            terminal_id=terminal_id,
            currency_code="RUB",
            final_success_prob=0.35,  # можно крутить
        )

    conn.commit()
    print("Injected amount-probing fraud:", info)


def generate_fraud_txn_batch(conn):
    """
    Один вызов — один фрод-сценарий (который создаёт несколько строк).
    """
    scenario = random.choice(
        [
            fraud_rapid_withdrawals_diff_terminals,
            fraud_after_card_closed,
            fraud_amount_probe,
        ]
    )
    scenario(conn)


# ===== БЭКФИЛЛ =====

def backfill_past_days(conn):
    """
    Генерация транзакций за предыдущие дни (BACKFILL_DAYS).
    """
    if BACKFILL_DAYS <= 0 or BACKFILL_TXNS_PER_DAY <= 0:
        return

    today = datetime.now(timezone.utc).date()
    for offset in range(1, BACKFILL_DAYS + 1):
        day = today - timedelta(days=offset)
        print(f"[BACKFILL] day={day}, txns={BACKFILL_TXNS_PER_DAY}")
        for _ in range(BACKFILL_TXNS_PER_DAY):
            insert_normal_txn_on_date(conn, day)


# ===== ГЛАВНАЯ ФУНКЦИЯ БАТЧА =====

def main():
    conn = get_conn()
    print("=== START BATCH ===")

    # 1. Новые клиенты/аккаунты/карты
    for _ in range(N_NEW_CUSTOMERS):
        create_customer_with_account_and_card(conn)

    # 2. Новые терминалы
    for _ in range(N_NEW_TERMINALS):
        create_terminal(conn)

    # 3. Закрываем старые карты
    for _ in range(N_CLOSE_CARDS):
        close_random_card(conn)

    # 4. Закрываем аккаунты
    for _ in range(N_CLOSE_ACCOUNTS):
        close_random_account(conn)

    # 5. Закрываем клиентов (у которых уже нет активных аккаунтов)
    for _ in range(N_CLOSE_CUSTOMERS):
        close_random_customer(conn)

    # 6. Закрываем терминалы
    for _ in range(N_CLOSE_TERMINALS):
        close_random_terminal(conn)

    # 7. Новые транзакции "сейчас": смесь нормальных и фродовых
    for _ in range(N_NEW_TXNS):
        # if random.random() < FRAUD_SHARE:
        # generate_fraud_txn_batch(conn)
        # else:
        insert_normal_txn_current(conn)
        if random.random() < FRAUD_SHARE:
            fraud_rapid_withdrawals_diff_terminals(conn)
        if random.random() < FRAUD_SHARE:
            fraud_after_card_closed(conn)
        if random.random() < FRAUD_SHARE:
            fraud_amount_probe(conn)

    # 8. Бэкфилл за прошлые дни
    # backfill_past_days(conn)

    conn.close()
    print("=== END BATCH ===")


if __name__ == "__main__":
    main()
