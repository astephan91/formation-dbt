import os
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/warehouse.duckdb")

def main():
    con = duckdb.connect(DUCKDB_PATH)
    # show a tiny report from dbt models
    queries = {
        "tables": "SHOW ALL TABLES;",
        "sample customers": "SELECT * FROM raw.customers LIMIT 5;",
        "sample orders": "SELECT * FROM raw.orders LIMIT 5;",
        "first model": "SELECT * FROM analytics.my_first_model LIMIT 5;"
    }
    for name, q in queries.items():
        print("\n===", name, "===")
        try:
            print(con.execute(q).df())
        except Exception as e:
            print(f"(query failed: {e})")
    con.close()

if __name__ == "__main__":
    main()
