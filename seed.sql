-- === Create some customers ===
INSERT INTO bank.customer (full_name, birth_date, email, phone, city)
SELECT
    'Customer ' || g AS full_name,
    DATE '1980-01-01' + (random() * 15000)::int AS birth_date,
    'customer' || g || '@example.com' AS email,
    '+1000000' || lpad(g::text, 4, '0') AS phone,
    (ARRAY['Moscow','SPB','Berlin','Prague','NYC'])[ (1 + floor(random()*5))::int ]
FROM generate_series(1, 100) AS g;

-- === Create 1–3 accounts per customer ===
INSERT INTO bank.account (account_number, customer_id, currency_code, opened_at, daily_transfer_limit)
SELECT
    'ACC' || lpad((1000000 + g)::text, 10, '0') AS account_number,
    c.customer_id,
    (ARRAY['RUB','USD','EUR'])[ (1 + floor(random()*3))::int ] AS currency_code,
    now() - (random()*365 || ' days')::interval AS opened_at,
    (ARRAY[50000, 100000, 250000])[ (1 + floor(random()*3))::int ]::numeric(18,2)
FROM bank.customer c
JOIN LATERAL generate_series(1, (1 + floor(random()*3))::int) AS g ON true;

-- === Create 1–2 cards per account ===
INSERT INTO bank.card (card_number, account_id, opened_at)
SELECT
    lpad((1000000000000000 + row_number() OVER ())::text, 16, '0') AS card_number,
    a.account_id,
    a.opened_at + (random()*60 || ' days')::interval
FROM bank.account a
JOIN LATERAL generate_series(1, (1 + floor(random()*2))::int) g ON true;

-- === Create some terminals / ATMs ===
INSERT INTO bank.terminal (terminal_code, city, country, latitude, longitude)
SELECT
    'ATM-' || lpad(g::text, 5, '0') AS terminal_code,
    (ARRAY['Moscow','SPB','Berlin','Prague','NYC'])[ (1 + floor(random()*5))::int ] AS city,
    (ARRAY['RU','DE','CZ','US'])[ (1 + floor(random()*4))::int ] AS country,
    (55 + random())::numeric(9,6),
    (37 + random())::numeric(9,6)
FROM generate_series(1, 50) AS g;
