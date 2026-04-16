from pathlib import Path

import duckdb

SQL = Path(__file__).parent.parent.parent / "sql"


def read_sql(filename: str) -> str:
    """Read a .sql file from the SQL_DIR and return its contents as a string."""
    sql_path = SQL / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


DB_PATH = Path(__file__).parent.parent.parent / "data" / "olist.duckdb"

conn = duckdb.connect(str(DB_PATH), read_only=True)
result = conn.sql(read_sql("Payment_information.sql"))
print(result)
