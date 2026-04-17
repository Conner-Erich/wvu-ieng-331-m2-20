from pathlib import Path

import duckdb
import polars as pl

SQL_PATH = Path(__file__).parent.parent.parent / "sql"
DB_PATH = Path(__file__).parent.parent.parent / "data"


def data_path(filename: str) -> Path:
    data_set = DB_PATH / filename
    if not data_set.exists():
        raise FileNotFoundError(f"Data set not found {data_set}")
    return Path(__file__).parent.parent.parent / "data" / filename


def read_sql(filename: str) -> str:
    """Read a .sql file and return its contents as a string."""
    sql_path = SQL_PATH / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


conn_payment_info = duckdb.connect(data_path("olist.duckdb"), read_only=True)
get_payment_information = conn_payment_info.sql(
    read_sql("Payment_information.sql"), params=[1]
).pl()

conn_price_shipping = duckdb.connect(data_path("olist.duckdb"), read_only=True)
get_price_shipping = conn_price_shipping.sql(
    read_sql("Price_shipping.sql"), params=["cheap"]
).pl()

conn_product_reviews = duckdb.connect(data_path("olist.duckdb"), read_only=True)
get_product_reviews = conn_product_reviews.sql(
    read_sql("Product_reviews.sql"), params=["poor"]
).pl()

conn_seller_con_loc = duckdb.connect(data_path("olist.duckdb"), read_only=True)
get_seller_consumer_location = conn_seller_con_loc.sql(
    read_sql("Seller_consumer_location.sql"), params=["alambari"]
).pl()


if __name__ == "__main__":
    print(get_seller_consumer_location)
