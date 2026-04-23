from __future__ import annotations

import json
import random
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd


def timed_query(conn: duckdb.DuckDBPyConnection, name: str, query: str) -> dict[str, float | int | str]:
    start = time.perf_counter()
    row_count = conn.execute(query).fetchone()[0]
    elapsed = time.perf_counter() - start
    return {"name": name, "elapsed_seconds": round(elapsed, 4), "row_count": int(row_count)}


def main() -> None:
    random.seed(7)

    root = Path(__file__).resolve().parents[2]
    db_path = root / "data" / "processed" / "tpcds_benchmark_smoke.duckdb"
    results_path = root / "experiments" / "results" / "tpcds_pipeline_results.json"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    results_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))

    customers = pd.DataFrame(
        {
            "customer_id": list(range(1, 5001)),
            "customer_segment": [random.choice(["bronze", "silver", "gold"]) for _ in range(5000)],
        }
    )
    stores = pd.DataFrame(
        {
            "store_id": list(range(1, 51)),
            "region": [random.choice(["east", "west", "central", "south"]) for _ in range(50)],
        }
    )
    items = pd.DataFrame(
        {
            "item_id": list(range(1, 1501)),
            "category": [random.choice(["electronics", "apparel", "home", "sports"]) for _ in range(1500)],
        }
    )
    base_date = date(2024, 1, 1)
    sales = pd.DataFrame(
        {
            "sale_id": list(range(1, 50001)),
            "customer_id": [random.randint(1, 5000) for _ in range(50000)],
            "store_id": [random.randint(1, 50) for _ in range(50000)],
            "item_id": [random.randint(1, 1500) for _ in range(50000)],
            "sales_date": [base_date + timedelta(days=random.randint(0, 364)) for _ in range(50000)],
            "sales_amount": [round(random.uniform(5.0, 500.0), 2) for _ in range(50000)],
        }
    )

    conn.register("customers_df", customers)
    conn.register("stores_df", stores)
    conn.register("items_df", items)
    conn.register("sales_df", sales)

    conn.execute("create or replace table customers as select * from customers_df")
    conn.execute("create or replace table stores as select * from stores_df")
    conn.execute("create or replace table items as select * from items_df")
    conn.execute("create or replace table store_sales as select * from sales_df")

    queries = [
        (
            "daily_store_sales",
            """
            create or replace table daily_store_sales as
            select
              store_id,
              sales_date,
              round(sum(sales_amount), 2) as total_sales,
              count(*) as txn_count
            from store_sales
            group by 1, 2;
            select count(*) from daily_store_sales;
            """,
        ),
        (
            "customer_ltv",
            """
            create or replace table customer_ltv as
            select
              c.customer_id,
              c.customer_segment,
              round(sum(s.sales_amount), 2) as lifetime_value,
              count(*) as purchases
            from customers c
            join store_sales s using (customer_id)
            group by 1, 2;
            select count(*) from customer_ltv;
            """,
        ),
        (
            "category_region_rollup",
            """
            create or replace table category_region_rollup as
            select
              st.region,
              i.category,
              round(sum(s.sales_amount), 2) as total_sales
            from store_sales s
            join stores st using (store_id)
            join items i using (item_id)
            group by 1, 2;
            select count(*) from category_region_rollup;
            """,
        ),
    ]

    results = [timed_query(conn, name, query) for name, query in queries]
    results_path.write_text(json.dumps(results, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
