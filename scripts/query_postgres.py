import os

import pandas as pd
import psycopg2


def main():
    con = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "dbt"),
        password=os.getenv("POSTGRES_PASSWORD", "dbt"),
        dbname=os.getenv("POSTGRES_DB", "warehouse"),
    )
    # show a tiny report from dbt models
    queries = {
        "tables": """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY 1, 2;
        """,
        "sample customers": "SELECT * FROM raw.customers LIMIT 5;",
        "sample orders": "SELECT * FROM raw.orders LIMIT 5;",
        "first model": "SELECT * FROM analytics.my_first_model LIMIT 5;"
    }
    for name, q in queries.items():
        print("\n===", name, "===")
        try:
            with con.cursor() as cur:
                cur.execute(q)
                print(pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description]))
        except Exception as e:
            con.rollback()
            print(f"(query failed: {e})")
    con.close()

if __name__ == "__main__":
    main()
