"""
TPC-DS-inspired core pipeline — 10-node, 4 domain tables.

DAG (depth → nodes):
  0 (raw):     raw_orders, raw_customers, raw_products
  1 (staging): stg_orders, stg_customers, stg_products
  2 (fact):    fct_sales, fct_inventory
  3 (report):  rpt_revenue, rpt_stock

Each run emits OpenLineage-compatible runtime events AND
a dbt-manifest-style design lineage record (schema, tests, contracts).
"""

import duckdb, uuid, json, datetime, os
from pathlib import Path

LINEAGE_DIR = Path("/tmp/lr_lineage")
LINEAGE_DIR.mkdir(exist_ok=True)

PIPELINE_EDGES = [
    ("raw_orders", "stg_orders"),
    ("raw_customers", "stg_customers"),
    ("raw_products", "stg_products"),
    ("stg_orders", "fct_sales"),
    ("stg_customers", "fct_sales"),
    ("stg_orders", "fct_inventory"),
    ("stg_products", "fct_inventory"),
    ("fct_sales", "rpt_revenue"),
    ("fct_inventory", "rpt_stock"),
]

NODE_DEPTH = {
    "raw_orders": 0, "raw_customers": 0, "raw_products": 0,
    "stg_orders": 1, "stg_customers": 1, "stg_products": 1,
    "fct_sales": 2, "fct_inventory": 2,
    "rpt_revenue": 3, "rpt_stock": 3,
}

# Design lineage: expected schema per node (simulates dbt manifest columns)
DESIGN_SCHEMA = {
    "raw_orders":    ["order_id", "customer_id", "product_id", "amount", "order_date"],
    "raw_customers": ["customer_id", "customer_name", "email", "country"],
    "raw_products":  ["product_id", "product_name", "unit_cost", "loaded_at"],
    "stg_orders":    ["order_id", "customer_id", "product_id", "amount", "order_date"],
    "stg_customers": ["customer_id", "customer_name", "country"],
    "stg_products":  ["product_id", "product_name", "unit_cost", "loaded_at"],
    "fct_sales":     ["order_id", "customer_id", "customer_name", "country", "product_id", "amount", "order_date"],
    "fct_inventory": ["product_id", "product_name", "unit_cost", "loaded_at", "order_count", "total_revenue"],
    "rpt_revenue":   ["country", "month", "order_count", "total_revenue", "avg_order_value"],
    "rpt_stock":     ["product_id", "product_name", "unit_cost", "order_count", "margin_ratio", "loaded_at"],
}

# Design lineage: contracts (expected invariants per node, simulates dbt tests + contracts)
DESIGN_CONTRACTS = {
    "raw_orders":    {"not_null": ["order_id"], "positive": ["amount"]},
    "raw_customers": {"not_null": ["customer_id", "customer_name"], "unique": ["customer_id"]},
    "raw_products":  {"not_null": ["product_id", "unit_cost"], "positive": ["unit_cost"]},
    "stg_orders":    {"not_null": ["order_id", "customer_id"], "positive": ["amount"]},
    "stg_products":  {"not_null": ["product_id"], "positive": ["unit_cost"]},
    "fct_sales":     {"not_null": ["order_id"], "positive": ["amount"]},
    "rpt_stock":     {"positive": ["unit_cost"]},
}


def now_iso():
    return datetime.datetime.utcnow().isoformat() + "Z"


class CorePipeline:
    def __init__(self, db_path="/tmp/lr_core.duckdb", row_scale=10_000, fault_config=None):
        self.db_path = db_path
        self.row_scale = row_scale
        self.fault_config = fault_config or {}
        self.conn = duckdb.connect(db_path)
        self.run_id = str(uuid.uuid4())
        self.node_stats = {}
        self.runtime_events = []   # OpenLineage-style runtime lineage
        self.schema_actual = {}    # actual schema per node (for drift detection)

    def _rows(self, t):
        try:
            return self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except Exception:
            return 0

    def _null_rate(self, t, col):
        try:
            total = self._rows(t)
            if total == 0: return 0.0
            n = self.conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL").fetchone()[0]
            return n / total
        except Exception:
            return 0.0

    def _actual_columns(self, t):
        try:
            cols = self.conn.execute(f"DESCRIBE {t}").fetchall()
            return [c[0] for c in cols]
        except Exception:
            return []

    def _emit_event(self, job, inputs, outputs, row_counts):
        ev = {
            "eventType": "COMPLETE",
            "eventTime": now_iso(),
            "run": {"runId": self.run_id},
            "job": {"namespace": "lineagerank", "name": job},
            "inputs": [{"namespace": "lineagerank", "name": i,
                        "facets": {"rowCount": row_counts.get(i)}} for i in inputs],
            "outputs": [{"namespace": "lineagerank", "name": o,
                         "facets": {"rowCount": row_counts.get(o)}} for o in outputs],
        }
        self.runtime_events.append(ev)

    def setup_raw(self):
        fc = self.fault_config
        n = self.row_scale

        # raw_orders — null_flood, row_drop, duplicate_load faults
        null_cust = fc.get("null_flood_customer_id", 0.0)
        row_scale_override = fc.get("_row_scale_override", n)
        dup_factor = 2 if fc.get("duplicate_load_orders", False) else 1

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE raw_orders AS
            SELECT i AS order_id,
                   CASE WHEN random() < {null_cust} THEN NULL
                        ELSE (i % 1000) + 1 END AS customer_id,
                   (i % 500) + 1 AS product_id,
                   (random()*500+10)::DECIMAL(10,2) AS amount,
                   CURRENT_DATE - INTERVAL ((random()*365)::INT) DAY AS order_date
            FROM generate_series(1, {row_scale_override}) t(i)
        """)
        if dup_factor > 1:
            # Simulate duplicate load: append all rows again (dedup not applied at raw layer)
            self.conn.execute(f"""
                INSERT INTO raw_orders
                SELECT * FROM raw_orders
            """)

        # raw_customers — fixed size (not scaled with row_scale: customer master is stable)
        # schema_drift fault removes email column
        N_CUSTOMERS = 1000
        if fc.get("schema_drift_customers", False):
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_customers AS
                SELECT i AS customer_id, 'Cust_'||i AS customer_name, 'US' AS country
                FROM generate_series(1, {N_CUSTOMERS}) t(i)
            """)
        else:
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE raw_customers AS
                SELECT i AS customer_id, 'Cust_'||i AS customer_name,
                       'u'||i||'@x.com' AS email, 'US' AS country
                FROM generate_series(1, {N_CUSTOMERS}) t(i)
            """)

        # raw_products — fixed size (product catalog is not affected by order volume)
        N_PRODUCTS = 500
        stale = fc.get("stale_partition_products", False)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE raw_products AS
            SELECT i AS product_id, 'Prod_'||i AS product_name,
                   (random()*100+5)::DECIMAL(10,2) AS unit_cost,
                   {'CURRENT_DATE - INTERVAL 90 DAY' if stale else 'CURRENT_DATE'} AS loaded_at
            FROM generate_series(1, {N_PRODUCTS}) t(i)
        """)

        for tbl in ["raw_orders", "raw_customers", "raw_products"]:
            self.node_stats[tbl] = {"row_count": self._rows(tbl)}
            self.schema_actual[tbl] = self._actual_columns(tbl)

        self.node_stats["raw_orders"]["null_rate_customer_id"] = \
            self._null_rate("raw_orders", "customer_id")
        if stale:
            self.node_stats["raw_products"]["freshness_lag_days"] = 90.0

    def run_stg_orders(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE stg_orders AS
            SELECT order_id, customer_id, product_id, amount, order_date
            FROM raw_orders WHERE order_id IS NOT NULL
        """)
        rc = self._rows("stg_orders")
        self.node_stats["stg_orders"] = {
            "row_count": rc,
            "null_rate_customer_id": self._null_rate("stg_orders", "customer_id"),
        }
        self.schema_actual["stg_orders"] = self._actual_columns("stg_orders")
        self._emit_event("stg_orders", ["raw_orders"], ["stg_orders"],
                         {"raw_orders": self._rows("raw_orders"), "stg_orders": rc})

    def run_stg_customers(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE stg_customers AS
            SELECT customer_id, customer_name, country FROM raw_customers
        """)
        rc = self._rows("stg_customers")
        self.node_stats["stg_customers"] = {"row_count": rc}
        self.schema_actual["stg_customers"] = self._actual_columns("stg_customers")
        self._emit_event("stg_customers", ["raw_customers"], ["stg_customers"],
                         {"raw_customers": self._rows("raw_customers"), "stg_customers": rc})

    def run_stg_products(self):
        fc = self.fault_config
        if fc.get("silent_value_corruption_cost", False):
            self.conn.execute("""
                CREATE OR REPLACE TABLE stg_products AS
                SELECT product_id, product_name, unit_cost * -1 AS unit_cost, loaded_at
                FROM raw_products
            """)
        else:
            self.conn.execute("""
                CREATE OR REPLACE TABLE stg_products AS
                SELECT product_id, product_name, unit_cost, loaded_at FROM raw_products
            """)
        rc = self._rows("stg_products")
        avg_cost_row = self.conn.execute("SELECT AVG(unit_cost) FROM stg_products").fetchone()
        avg_cost = avg_cost_row[0] if avg_cost_row and avg_cost_row[0] is not None else 0.0
        self.node_stats["stg_products"] = {"row_count": rc, "avg_unit_cost": avg_cost}
        self.schema_actual["stg_products"] = self._actual_columns("stg_products")
        self._emit_event("stg_products", ["raw_products"], ["stg_products"],
                         {"raw_products": self._rows("raw_products"), "stg_products": rc})

    def run_fct_sales(self):
        fc = self.fault_config
        if fc.get("bad_join_sales", False):
            # Bad join: use order_id as join key instead of customer_id.
            # order_ids go 1..N (up to 10k), customer_ids go 1..1000.
            # Only orders with order_id ≤ 1000 match → ~90% null_rate_customer_name.
            self.conn.execute("""
                CREATE OR REPLACE TABLE fct_sales AS
                SELECT o.order_id, o.customer_id,
                       c.customer_name, c.country,
                       o.product_id, o.amount, o.order_date
                FROM stg_orders o
                LEFT JOIN stg_customers c ON o.order_id = c.customer_id
            """)
        else:
            self.conn.execute("""
                CREATE OR REPLACE TABLE fct_sales AS
                SELECT o.order_id, o.customer_id,
                       c.customer_name, c.country,
                       o.product_id, o.amount, o.order_date
                FROM stg_orders o
                LEFT JOIN stg_customers c ON o.customer_id = c.customer_id
            """)
        rc = self._rows("fct_sales")
        self.node_stats["fct_sales"] = {
            "row_count": rc,
            "null_rate_customer_name": self._null_rate("fct_sales", "customer_name"),
        }
        self.schema_actual["fct_sales"] = self._actual_columns("fct_sales")
        self._emit_event("fct_sales", ["stg_orders", "stg_customers"], ["fct_sales"],
                         {"stg_orders": self._rows("stg_orders"),
                          "stg_customers": self._rows("stg_customers"), "fct_sales": rc})

    def run_fct_inventory(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE fct_inventory AS
            SELECT p.product_id, p.product_name, p.unit_cost, p.loaded_at,
                   COUNT(o.order_id) AS order_count, SUM(o.amount) AS total_revenue
            FROM stg_products p
            LEFT JOIN stg_orders o ON p.product_id = o.product_id
            GROUP BY 1,2,3,4
        """)
        rc = self._rows("fct_inventory")
        self.node_stats["fct_inventory"] = {"row_count": rc}
        self.schema_actual["fct_inventory"] = self._actual_columns("fct_inventory")
        self._emit_event("fct_inventory", ["stg_products", "stg_orders"], ["fct_inventory"],
                         {"stg_products": self._rows("stg_products"),
                          "stg_orders": self._rows("stg_orders"), "fct_inventory": rc})

    def run_reports(self):
        self.conn.execute("""
            CREATE OR REPLACE TABLE rpt_revenue AS
            SELECT country, DATE_TRUNC('month', order_date) AS month,
                   COUNT(*) AS order_count, SUM(amount) AS total_revenue,
                   AVG(amount) AS avg_order_value
            FROM fct_sales GROUP BY 1,2
        """)
        self.conn.execute("""
            CREATE OR REPLACE TABLE rpt_stock AS
            SELECT p.product_id, p.product_name, p.unit_cost,
                   COUNT(s.order_id) AS order_count,
                   p.unit_cost * 1.5 AS margin_ratio, p.loaded_at
            FROM stg_products p
            LEFT JOIN fct_sales s ON p.product_id = s.product_id
            GROUP BY 1,2,3,6
        """)
        for tbl in ["rpt_revenue", "rpt_stock"]:
            self.node_stats[tbl] = {"row_count": self._rows(tbl)}
            self.schema_actual[tbl] = self._actual_columns(tbl)

        # Contract checks
        neg_cost = self.conn.execute(
            "SELECT COUNT(*) FROM rpt_stock WHERE unit_cost < 0").fetchone()[0]
        high_null_sales = self._null_rate("fct_sales", "customer_name") > 0.30
        dup_ratio = (self._rows("raw_orders") / max(self._rows("stg_orders"), 1))
        duplicate_detected = dup_ratio > 1.8

        return {
            "rpt_stock": neg_cost == 0 and not duplicate_detected,
            "rpt_revenue": not high_null_sales and not duplicate_detected,
        }

    def run_all(self):
        self.setup_raw()
        self.run_stg_orders()
        self.run_stg_customers()
        self.run_stg_products()
        self.run_fct_sales()
        self.run_fct_inventory()
        tests = self.run_reports()
        return {
            "run_id": self.run_id,
            "node_stats": self.node_stats,
            "schema_actual": self.schema_actual,
            "runtime_events": self.runtime_events,
            "tests": tests,
        }

    def close(self):
        self.conn.close()
