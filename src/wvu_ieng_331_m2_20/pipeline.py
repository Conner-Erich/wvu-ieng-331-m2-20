import argparse
from pathlib import Path

import duckdb
import polars as pl
from queries import (
    DB_PATH,
    get_payment_information,
    get_price_shipping,
    get_product_reviews,
    get_seller_consumer_location,
)
from validation import (
    check_columns_not_null,
    check_date_range,
    check_row_counts,
    check_tables_exist,
)


def parse_args() -> argparse.Namespace:
    """creates the arguments for the database, and for the queries payment_installment, shipping_price_rating, review_rating, and seller city."""
    parser = argparse.ArgumentParser(
        prog="pipeline.py",
        description="Olist database reporting.",
        epilog="Running with no arguments produces the preset data values.",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="olist.duckdb",
        metavar="FILENAME",
        help="Database filename inside the /data folder. (default: olist.duckdb)",
    )
    parser.add_argument(
        "--payment_installment",
        type=int,
        default=1,
        help="Filter by number of payment installments. (default: 1)",
    )
    parser.add_argument(
        "--shipping_price_rating",
        type=str,
        default="cheap",
        help="Filter by shipping price rating e.g. cheap, expensive. (default: cheap)",
    )
    parser.add_argument(
        "--review_rating",
        type=str,
        default="bad",
        help="Filter by review rating e.g. bad, good. (default: bad)",
    )
    parser.add_argument(
        "--seller_city",
        type=str,
        default="sao paulo",
        help="Filter by seller city. (default: sao paulo)",
    )

    return parser.parse_args()


def get_connection(db_filename: str = "olist.duckdb") -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to the given database file."""
    db_path = Path(__file__).parent.parent.parent / "data" / db_filename
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def run_validation(conn: duckdb.DuckDBPyConnection) -> None:
    """Run all data quality checks before the main analysis."""
    check_tables_exist(conn)
    check_columns_not_null(conn)
    check_date_range(conn)
    check_row_counts(conn)


def pipeline() -> None:
    """applys arguments, validates data, and runs the analysis."""

    args = parse_args()

    conn = get_connection()

    run_validation(conn)

    df_payments = get_payment_information(conn, args.payment_installment)
    df_cities = get_seller_consumer_location(conn, args.seller_city)

    print("Payment Information")
    print(df_payments)

    print("City Products")
    print(df_cities)


if __name__ == "__main__":
    pipeline()
