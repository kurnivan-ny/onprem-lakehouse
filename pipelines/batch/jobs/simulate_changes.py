"""Make the source OLTP 'live' so the next batch run is incremental:  make batch-simulate"""
import argparse
import os

from sqlalchemy import create_engine, text

SQL_NEW_ORDERS = """
WITH new_orders AS (
    INSERT INTO sales.orders (customer_id, status)
    SELECT 1 + floor(random()*500)::int, 'pending' FROM generate_series(1, :n)
    RETURNING order_id
)
INSERT INTO sales.order_items (order_id, product_id, quantity, unit_price)
SELECT o.order_id, p.product_id, 1 + floor(random()*3)::int, p.price
FROM new_orders o
CROSS JOIN LATERAL (SELECT product_id, price FROM sales.products
                    WHERE o.order_id > 0 ORDER BY random() LIMIT 2) p;
"""
SQL_ADVANCE = """
UPDATE sales.orders SET status = CASE status WHEN 'pending' THEN 'paid' WHEN 'paid' THEN 'shipped' END
WHERE order_id IN (SELECT order_id FROM sales.orders WHERE status IN ('pending','paid')
                   ORDER BY random() LIMIT :n);
"""
SQL_EDIT_CUSTOMER = """
UPDATE sales.customers SET email = '  NEW.' || email
WHERE customer_id = (SELECT customer_id FROM sales.customers ORDER BY random() LIMIT 1);
"""


def engine():
    return create_engine(
        f"postgresql+psycopg://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
        f"@{os.getenv('PG_HOST', 'source-db')}:{os.getenv('PG_PORT', '5432')}/{os.environ['PG_DB']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=25)
    n = ap.parse_args().orders
    with engine().begin() as conn:
        conn.execute(text(SQL_NEW_ORDERS), {"n": n})
        conn.execute(text(SQL_ADVANCE), {"n": n})
        conn.execute(text(SQL_EDIT_CUSTOMER))
    print(f"inserted {n} orders, advanced {n} statuses, edited 1 customer")
