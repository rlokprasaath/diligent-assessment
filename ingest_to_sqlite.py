import sqlite3
from pathlib import Path
from typing import List, Sequence

import pandas as pd

DATA_DIR = Path("data")
DB_PATH = Path("ecommerce.db")

DROP_ORDER = ["payments", "order_items", "orders", "products", "users"]

CREATE_STATEMENTS = {
    "users": """
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            signup_date TEXT NOT NULL,
            phone_number TEXT UNIQUE
        );
    """,
    "products": """
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT CHECK(category IN ('electronics','fashion','home','beauty','books','sports')),
            price REAL CHECK(price > 0),
            stock_quantity INTEGER CHECK(stock_quantity >= 0)
        );
    """,
    "orders": """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL CHECK(total_amount > 0),
            order_status TEXT CHECK(order_status IN ('pending','completed','cancelled')),
            FOREIGN KEY(user_id) REFERENCES users(user_id)
        );
    """,
    "order_items": """
        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER CHECK(quantity > 0),
            unit_price REAL CHECK(unit_price > 0),
            line_total REAL CHECK(line_total >= 0),
            FOREIGN KEY(order_id) REFERENCES orders(order_id),
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        );
    """,
    "payments": """
        CREATE TABLE payments (
            payment_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_method TEXT CHECK(payment_method IN ('credit_card','debit_card','upi','net_banking')),
            payment_status TEXT CHECK(payment_status IN ('successful','failed','pending')),
            payment_date TEXT NOT NULL,
            amount_paid REAL CHECK(amount_paid > 0),
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        );
    """,
}

LOAD_SEQUENCE = [
    ("users", "users.csv", ["user_id", "full_name", "email", "signup_date", "phone_number"]),
    (
        "products",
        "products.csv",
        ["product_id", "product_name", "category", "price", "stock_quantity"],
    ),
    (
        "orders",
        "orders.csv",
        ["order_id", "user_id", "order_date", "total_amount", "order_status"],
    ),
    (
        "order_items",
        "order_items.csv",
        ["item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"],
    ),
    (
        "payments",
        "payments.csv",
        ["payment_id", "order_id", "payment_method", "payment_status", "payment_date", "amount_paid"],
    ),
]


def ensure_data_dir(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Data directory '{path}' does not exist. Run generate_data.py first.")


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def drop_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    print("Dropping existing tables (if any)...")
    for table in DROP_ORDER:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
    conn.commit()


def create_tables(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()
    print("Creating tables with constraints...")
    for table in ["users", "products", "orders", "order_items", "payments"]:
        cursor.execute(CREATE_STATEMENTS[table])
    conn.commit()


def load_dataframe(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df.where(pd.notnull(df), None)


def dataframe_to_records(df: pd.DataFrame, columns: Sequence[str]) -> List[tuple]:
    return [tuple(row[col] for col in columns) for _, row in df.iterrows()]


def insert_data(conn: sqlite3.Connection) -> None:
    cursor = conn.cursor()

    for table_name, csv_name, columns in LOAD_SEQUENCE:
        csv_path = DATA_DIR / csv_name
        print(f"Loading data from {csv_path} into '{table_name}'...")
        df = load_dataframe(csv_path)
        records = dataframe_to_records(df, columns)
        placeholders = ", ".join(["?"] * len(columns))
        column_list = ", ".join(columns)
        cursor.executemany(
            f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders});",
            records,
        )
        print(f"Inserted {len(records)} rows into '{table_name}'.")

    conn.commit()


def main():
    ensure_data_dir(DATA_DIR)
    print(f"Connecting to SQLite database at '{DB_PATH}'...")
    conn = get_connection(DB_PATH)
    try:
        drop_tables(conn)
        create_tables(conn)
        insert_data(conn)
        print("SQLite ingestion completed successfully.")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()

