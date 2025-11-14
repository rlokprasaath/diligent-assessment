import argparse
import random
from collections import defaultdict
from pathlib import Path
from typing import Tuple

import pandas as pd
from faker import Faker

CATEGORIES = ["electronics", "fashion", "home", "beauty", "books", "sports"]
ORDER_STATUSES = ["pending", "completed", "cancelled"]
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "net_banking"]
PAYMENT_STATUSES = ["successful", "failed", "pending"]


def generate_users(num_users: int, faker: Faker) -> pd.DataFrame:
    print(f"Generating {num_users} users...")
    users = []

    for user_id in range(1, num_users + 1):
        full_name = faker.name()
        email = faker.unique.email()
        phone = faker.unique.msisdn()
        signup_date = faker.date_between(start_date="-2y", end_date="today")

        users.append(
            {
                "user_id": user_id,
                "full_name": full_name,
                "email": email,
                "signup_date": signup_date,
                "phone_number": phone,
            }
        )

    faker.unique.clear()
    return pd.DataFrame(users)


def generate_products(num_products: int, faker: Faker) -> pd.DataFrame:
    print(f"Generating {num_products} products...")
    products = []
    for product_id in range(1, num_products + 1):
        category = random.choice(CATEGORIES)
        base_price = round(random.uniform(5.0, 500.0), 2)
        products.append(
            {
                "product_id": product_id,
                "product_name": f"{faker.color_name()} {faker.word().title()}",
                "category": category,
                "price": base_price,
                "stock_quantity": random.randint(0, 500),
            }
        )
    return pd.DataFrame(products)


def generate_orders(num_orders: int, users_df: pd.DataFrame, faker: Faker) -> pd.DataFrame:
    print(f"Generating {num_orders} orders...")
    orders = []
    for order_id in range(1, num_orders + 1):
        user = users_df.sample(1).iloc[0]
        order_date = faker.date_between(
            start_date=user["signup_date"], end_date="today"
        )
        order_status = random.choices(ORDER_STATUSES, weights=[0.2, 0.7, 0.1])[0]
        orders.append(
            {
                "order_id": order_id,
                "user_id": int(user["user_id"]),
                "order_date": order_date,
                "total_amount": 0.0,
                "order_status": order_status,
            }
        )
    return pd.DataFrame(orders)


def generate_order_items(
    orders_df: pd.DataFrame, products_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.Series]:
    print("Generating order items...")
    order_totals = defaultdict(float)
    items = []
    item_id = 1

    for _, order in orders_df.iterrows():
        num_items = random.randint(1, 5)
        selected_products = products_df.sample(num_items, replace=True)

        for _, product in selected_products.iterrows():
            quantity = random.randint(1, 5)
            unit_price = round(
                random.uniform(product["price"] * 0.9, product["price"] * 1.1), 2
            )
            line_total = round(quantity * unit_price, 2)

            items.append(
                {
                    "item_id": item_id,
                    "order_id": int(order["order_id"]),
                    "product_id": int(product["product_id"]),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

            order_totals[order["order_id"]] += line_total
            item_id += 1

    items_df = pd.DataFrame(items)
    order_totals_series = pd.Series(order_totals)
    return items_df, order_totals_series


def generate_payments(orders_df: pd.DataFrame, faker: Faker) -> pd.DataFrame:
    print("Generating payments...")
    payments = []
    for order in orders_df.itertuples():
        payment_status = random.choices(
            PAYMENT_STATUSES, weights=[0.75, 0.15, 0.1]
        )[0]
        payment_method = random.choice(PAYMENT_METHODS)

        payment_date = faker.date_between(
            start_date=order.order_date, end_date="today"
        )

        if payment_status == "successful":
            amount_paid = round(order.total_amount, 2)
        else:
            amount_paid = round(
                random.uniform(1.0, max(order.total_amount, 1.0)), 2
            )

        payments.append(
            {
                "payment_id": order.order_id,
                "order_id": order.order_id,
                "payment_method": payment_method,
                "payment_status": payment_status,
                "payment_date": payment_date,
                "amount_paid": max(amount_paid, 0.01),
            }
        )

    return pd.DataFrame(payments)


def ensure_data_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce datasets."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=300,
        help="Approximate number of orders to generate (default: 300)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducibility.",
    )
    args = parser.parse_args()

    if args.rows <= 0:
        raise ValueError("rows must be a positive integer.")

    seed = args.seed if args.seed is not None else random.randint(1, 1_000_000)
    print(f"Using random seed: {seed}")
    random.seed(seed)
    faker = Faker()
    faker.seed_instance(seed)

    num_orders = args.rows
    num_users = max(int(num_orders * 0.6), 50)
    num_products = max(int(num_orders * 0.5), 40)

    data_dir = Path("data")
    ensure_data_dir(data_dir)

    users_df = generate_users(num_users, faker)
    products_df = generate_products(num_products, faker)
    orders_df = generate_orders(num_orders, users_df, faker)
    order_items_df, order_totals = generate_order_items(orders_df, products_df)

    print("Updating order totals...")
    orders_df = orders_df.set_index("order_id")
    orders_df["total_amount"] = order_totals.round(2)
    orders_df.reset_index(inplace=True)

    payments_df = generate_payments(orders_df, faker)

    print("Exporting CSV files...")
    users_df.to_csv(data_dir / "users.csv", index=False)
    products_df.to_csv(data_dir / "products.csv", index=False)
    orders_df.to_csv(data_dir / "orders.csv", index=False)
    order_items_df.to_csv(data_dir / "order_items.csv", index=False)
    payments_df.to_csv(data_dir / "payments.csv", index=False)

    print(f"Data generation complete. Files saved to {data_dir.resolve()}")


if __name__ == "__main__":
    main()

