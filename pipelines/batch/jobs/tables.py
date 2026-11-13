"""Source tables (Postgres schema `sales`) and their primary keys. Watermark column = updated_at.

Lake naming: <layer>.sales_<table>, e.g. lakehouse.bronze.sales_orders
"""
SOURCE_SCHEMA = "sales"
DOMAIN = "sales"

SOURCE_TABLES = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
}


def lake_name(table: str) -> str:
    return f"{DOMAIN}_{table}"
