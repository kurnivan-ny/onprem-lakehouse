-- Sample OLTP source (e-commerce). Runs once on first Postgres start.
CREATE SCHEMA IF NOT EXISTS sales;
SET search_path TO sales;

CREATE TABLE customers (
    customer_id  SERIAL PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    email        TEXT        NOT NULL,
    country      CHAR(2)     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    product_id   SERIAL PRIMARY KEY,
    name         TEXT          NOT NULL,
    category     TEXT          NOT NULL,
    price        NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT         NOT NULL REFERENCES customers(customer_id),
    order_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status       TEXT        NOT NULL CHECK (status IN ('pending','paid','shipped','cancelled')),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT           NOT NULL REFERENCES orders(order_id),
    product_id    INT           NOT NULL REFERENCES products(product_id),
    quantity      INT           NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(12,2) NOT NULL,
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- updated_at is the incremental watermark, so keep it honest on every UPDATE
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END; $$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers   BEFORE UPDATE ON customers   FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_products    BEFORE UPDATE ON products    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_orders      BEFORE UPDATE ON orders      FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER trg_order_items BEFORE UPDATE ON order_items FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

CREATE INDEX ix_customers_upd   ON customers(updated_at);
CREATE INDEX ix_products_upd    ON products(updated_at);
CREATE INDEX ix_orders_upd      ON orders(updated_at);
CREATE INDEX ix_order_items_upd ON order_items(updated_at);

-- ---------- seed data ----------
INSERT INTO customers (full_name, email, country, created_at)
SELECT 'Customer ' || g,
       'Customer' || g || '@Example.COM ',                       -- messy on purpose: silver cleans it
       (ARRAY['ID','SG','MY','TH','VN','PH'])[1 + floor(random()*6)::int],
       now() - (random() * interval '180 days')
FROM generate_series(1, 500) g;

INSERT INTO products (name, category, price)
SELECT 'Product ' || g,
       (ARRAY['electronics','fashion','home','beauty','sports'])[1 + floor(random()*5)::int],
       round((10 + random()*490)::numeric, 2)
FROM generate_series(1, 60) g;

INSERT INTO orders (customer_id, order_ts, status)
SELECT 1 + floor(random()*500)::int,
       now() - (random() * interval '90 days'),
       (ARRAY['pending','paid','paid','shipped','shipped','cancelled'])[1 + floor(random()*6)::int]
FROM generate_series(1, 3000) g;

INSERT INTO order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id, p.product_id, 1 + floor(random()*3)::int, p.price
FROM orders o
CROSS JOIN LATERAL (
    SELECT product_id, price FROM products
    WHERE o.order_id > 0              -- correlate so each order gets its own random picks
    ORDER BY random() LIMIT 1 + floor(random()*3)::int
) p;
