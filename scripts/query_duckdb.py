import os
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/warehouse.duckdb")

def main():
    con = duckdb.connect(DUCKDB_PATH)
    # show a tiny report from dbt models
    queries = {
        "tables": "SHOW ALL TABLES;",
        "sample_dim_customers": "SELECT * FROM analytics_analytics.dim_customers LIMIT 5;",
        "sample_fct_orders": "SELECT * FROM analytics_analytics.fct_orders LIMIT 5;",
        "top_customers": "SELECT * FROM analytics_analytics.rpt_customer_revenue ORDER BY total_revenue DESC LIMIT 10;",
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
