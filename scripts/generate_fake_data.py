import os
import random
from datetime import date, timedelta

import pandas as pd
import psycopg2
from faker import Faker
from psycopg2.extras import execute_values


RAW_TABLES = {
    "customers": """
        customer_id TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        country TEXT,
        signup_date DATE
    """,
    "orders": """
        order_id TEXT,
        customer_id TEXT,
        order_date DATE,
        amount NUMERIC(10, 2),
        status TEXT
    """,
    "order_items": """
        item_id TEXT,
        order_id TEXT,
        product_id TEXT,
        quantity INTEGER,
        unit_price NUMERIC(10, 2)
    """,
}


def load_table(cur, name: str, df: pd.DataFrame) -> None:
    # Truncate instead of drop so that dbt views built on top of raw tables survive
    cur.execute(f"CREATE TABLE IF NOT EXISTS raw.{name} ({RAW_TABLES[name]});")
    cur.execute(f"TRUNCATE TABLE raw.{name};")
    columns = ", ".join(df.columns)
    rows = list(df.astype(object).itertuples(index=False, name=None))
    execute_values(cur, f"INSERT INTO raw.{name} ({columns}) VALUES %s", rows)


def main() -> None:
    n_customers = int(os.getenv("CUSTOMERS", "100"))
    n_orders = int(os.getenv("ORDERS", "1000"))
    seed = int(os.getenv("SEED", "42"))

    random.seed(seed)
    fake = Faker()
    Faker.seed(seed)

    # --- customers
    customers = []
    countries = ["FR", "DE", "ES", "IT", "GB", "US", "CA", "NL", "BE", "CH"]
    for i in range(1, n_customers + 1):
        first = fake.first_name()
        last = fake.last_name()
        customers.append(
            {
                "customer_id": f"C{i:05d}",
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}@example.com".replace("'", ""),
                "country": random.choice(countries),
                "signup_date": fake.date_between(start_date="-3y", end_date="today"),
            }
        )
    customers_df = pd.DataFrame(customers)

    # --- orders
    orders = []
    statuses = ["created", "paid", "shipped", "cancelled", "refunded"]
    start = date.today() - timedelta(days=365)
    for i in range(1, n_orders + 1):
        cust = random.choice(customers_df["customer_id"].tolist())
        order_dt = start + timedelta(days=random.randint(0, 365))
        amount = round(max(5.0, random.gauss(120, 50)), 2)
        status = random.choices(
            statuses,
            weights=[0.15, 0.45, 0.25, 0.10, 0.05],
            k=1,
        )[0]
        orders.append(
            {
                "order_id": f"O{i:06d}",
                "customer_id": cust,
                "order_date": order_dt.isoformat(),
                "amount": amount,
                "status": status,
            }
        )
    orders_df = pd.DataFrame(orders)

    # --- order_items
    product_ids = [f"P{i:03d}" for i in range(1, 21)]
    product_prices = {
        "P001": 89.99, "P002": 34.99, "P003": 129.99, "P004": 79.99,
        "P005": 54.99, "P006": 199.99, "P007": 39.99, "P008": 29.99,
        "P009": 109.99, "P010": 19.99, "P011": 69.99, "P012": 24.99,
        "P013": 49.99, "P014": 34.99, "P015": 44.99, "P016": 59.99,
        "P017": 22.99, "P018": 39.99, "P019": 64.99, "P020": 34.99,
    }
    order_items = []
    item_counter = 1
    for _, order in orders_df.iterrows():
        n_items = random.randint(1, 4)
        selected_products = random.sample(product_ids, min(n_items, len(product_ids)))
        for product_id in selected_products:
            quantity = random.randint(1, 3)
            unit_price = product_prices[product_id]
            order_items.append({
                "item_id": f"I{item_counter:07d}",
                "order_id": order["order_id"],
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
            })
            item_counter += 1
    order_items_df = pd.DataFrame(order_items)

    con = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "dbt"),
        password=os.getenv("POSTGRES_PASSWORD", "dbt"),
        dbname=os.getenv("POSTGRES_DB", "warehouse"),
    )
    with con, con.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        load_table(cur, "customers", customers_df)
        load_table(cur, "orders", orders_df)
        load_table(cur, "order_items", order_items_df)
    con.close()
    print(
        f"Wrote raw tables to PostgreSQL: "
        f"raw.customers({n_customers}), raw.orders({n_orders}), "
        f"raw.order_items({len(order_items_df)})"
    )


if __name__ == "__main__":
    main()
