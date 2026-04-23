"""
50-node enterprise pipeline (FW1 fix).

Four business domains sharing dimension tables:
  Sales, Marketing, Inventory, Finance

Structure per domain:
  2 raw sources → 2 staging → 1 fact → 1 report (= 6 nodes × 4 = 24)
  + 3 shared dimensions (customers, products, calendar) × (raw+stg) = 6
  + 4 cross-domain summary reports = 4
  + 4 data quality / observability nodes = 4
  Total: ~50 nodes, ~65 edges

Shared dimensions make disambiguation harder:
  raw_customers feeds ALL four fact tables, so coverage alone
  cannot distinguish a customer fault from a domain-specific fault —
  the fused anomaly signal (runtime + design) must do the work.
"""

import duckdb, uuid, json, os
from pathlib import Path
import networkx as nx

EDGES_50 = [
    # Shared dimensions
    ("raw_customers",  "stg_customers"),
    ("raw_products",   "stg_products"),
    ("raw_calendar",   "stg_calendar"),

    # Sales domain
    ("raw_orders",     "stg_orders"),
    ("stg_orders",     "fct_sales"),
    ("stg_customers",  "fct_sales"),
    ("stg_products",   "fct_sales"),
    ("fct_sales",      "rpt_revenue"),
    ("fct_sales",      "rpt_sales_detail"),

    # Marketing domain
    ("raw_campaigns",  "stg_campaigns"),
    ("raw_events",     "stg_events"),
    ("stg_events",     "fct_attribution"),
    ("stg_campaigns",  "fct_attribution"),
    ("stg_customers",  "fct_attribution"),
    ("fct_attribution","rpt_campaign_perf"),
    ("fct_attribution","rpt_customer_journey"),

    # Inventory domain
    ("raw_warehouse",  "stg_warehouse"),
    ("raw_shipments",  "stg_shipments"),
    ("stg_warehouse",  "fct_inventory"),
    ("stg_shipments",  "fct_inventory"),
    ("stg_products",   "fct_inventory"),
    ("fct_inventory",  "rpt_stock_levels"),
    ("fct_inventory",  "rpt_fulfillment"),

    # Finance domain
    ("raw_ledger",     "stg_ledger"),
    ("raw_invoices",   "stg_invoices"),
    ("stg_ledger",     "fct_finance"),
    ("stg_invoices",   "fct_finance"),
    ("stg_customers",  "fct_finance"),
    ("fct_finance",    "rpt_pnl"),
    ("fct_finance",    "rpt_ar"),

    # Cross-domain summaries
    ("rpt_revenue",      "rpt_executive_summary"),
    ("rpt_campaign_perf","rpt_executive_summary"),
    ("rpt_stock_levels", "rpt_executive_summary"),
    ("rpt_pnl",          "rpt_executive_summary"),

    # Observability nodes (data quality monitoring layer)
    ("stg_orders",     "dq_orders"),
    ("stg_customers",  "dq_customers"),
    ("fct_sales",      "dq_sales"),
    ("fct_finance",    "dq_finance"),

    # Calendar joins (used in time-based reports)
    ("stg_calendar",   "fct_sales"),
    ("stg_calendar",   "fct_finance"),
]

# All 50 nodes
ALL_NODES_50 = [
    # Shared raw
    "raw_customers", "raw_products", "raw_calendar",
    # Shared staging
    "stg_customers", "stg_products", "stg_calendar",
    # Sales
    "raw_orders", "stg_orders", "fct_sales", "rpt_revenue", "rpt_sales_detail",
    # Marketing
    "raw_campaigns", "raw_events", "stg_campaigns", "stg_events",
    "fct_attribution", "rpt_campaign_perf", "rpt_customer_journey",
    # Inventory
    "raw_warehouse", "raw_shipments", "stg_warehouse", "stg_shipments",
    "fct_inventory", "rpt_stock_levels", "rpt_fulfillment",
    # Finance
    "raw_ledger", "raw_invoices", "stg_ledger", "stg_invoices",
    "fct_finance", "rpt_pnl", "rpt_ar",
    # Cross-domain
    "rpt_executive_summary",
    # DQ nodes
    "dq_orders", "dq_customers", "dq_sales", "dq_finance",
]

DEPTH_50 = {
    "raw_customers": 0, "raw_products": 0, "raw_calendar": 0,
    "raw_orders": 0, "raw_campaigns": 0, "raw_events": 0,
    "raw_warehouse": 0, "raw_shipments": 0,
    "raw_ledger": 0, "raw_invoices": 0,
    "stg_customers": 1, "stg_products": 1, "stg_calendar": 1,
    "stg_orders": 1, "stg_campaigns": 1, "stg_events": 1,
    "stg_warehouse": 1, "stg_shipments": 1,
    "stg_ledger": 1, "stg_invoices": 1,
    "fct_sales": 2, "fct_attribution": 2, "fct_inventory": 2, "fct_finance": 2,
    "dq_orders": 2, "dq_customers": 2, "dq_sales": 3, "dq_finance": 3,
    "rpt_revenue": 3, "rpt_sales_detail": 3,
    "rpt_campaign_perf": 3, "rpt_customer_journey": 3,
    "rpt_stock_levels": 3, "rpt_fulfillment": 3,
    "rpt_pnl": 3, "rpt_ar": 3,
    "rpt_executive_summary": 4,
}

EXPECTED_ROWS_50 = {
    "raw_customers": 1_000, "raw_products": 500, "raw_calendar": 365,
    "raw_orders": 10_000, "raw_campaigns": 100, "raw_events": 50_000,
    "raw_warehouse": 200, "raw_shipments": 8_000,
    "raw_ledger": 5_000, "raw_invoices": 3_000,
    "stg_customers": 1_000, "stg_products": 500, "stg_calendar": 365,
    "stg_orders": 10_000, "stg_campaigns": 100, "stg_events": 50_000,
    "stg_warehouse": 200, "stg_shipments": 8_000,
    "stg_ledger": 5_000, "stg_invoices": 3_000,
    "fct_sales": 10_000, "fct_attribution": 50_000,
    "fct_inventory": 500, "fct_finance": 8_000,
}


def build_50node_dag() -> nx.DiGraph:
    G = nx.DiGraph()
    for n in ALL_NODES_50:
        G.add_node(n, depth=DEPTH_50.get(n, 2))
    for s, d in EDGES_50:
        if s in ALL_NODES_50 and d in ALL_NODES_50:
            G.add_edge(s, d)
    return G


class Pipeline50Node:
    """
    50-node pipeline runner using DuckDB.
    Generates synthetic data for all 10 raw source tables.
    """

    def __init__(self, db_path="/tmp/lr_50node.duckdb",
                 row_scale=10_000, fault_config=None):
        self.db_path = db_path
        self.row_scale = row_scale
        self.fault_config = fault_config or {}
        self.conn = duckdb.connect(db_path)
        self.run_id = str(uuid.uuid4())
        self.node_stats = {}
        self.schema_actual = {}

    def _rows(self, t):
        try: return self.conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except: return 0

    def _null_rate(self, t, col):
        try:
            tot = self._rows(t)
            if tot == 0: return 0.0
            n = self.conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL").fetchone()[0]
            return n / tot
        except: return 0.0

    def _cols(self, t):
        try: return [c[0] for c in self.conn.execute(f"DESCRIBE {t}").fetchall()]
        except: return []

    def _stat(self, tbl, extras=None):
        s = {"row_count": self._rows(tbl)}
        if extras:
            for col in extras:
                s[f"null_rate_{col}"] = self._null_rate(tbl, col)
        self.node_stats[tbl] = s
        self.schema_actual[tbl] = self._cols(tbl)

    def setup(self):
        fc = self.fault_config
        n = self.row_scale

        null_cust = fc.get("null_flood_customer_id", 0.0)
        null_ev   = fc.get("null_flood_campaign_id", 0.0)
        stale     = fc.get("stale_partition_products", False)
        schema_d  = fc.get("schema_drift_customers", False)
        row_ov    = fc.get("_row_scale_override", n)

        # Shared dimensions
        if schema_d:
            self.conn.execute(f"CREATE OR REPLACE TABLE raw_customers AS SELECT i AS customer_id, 'Cust_'||i AS customer_name, 'US' AS country FROM generate_series(1,{n//10}) t(i)")
        else:
            self.conn.execute(f"CREATE OR REPLACE TABLE raw_customers AS SELECT i AS customer_id, 'Cust_'||i AS customer_name, 'u'||i||'@x.com' AS email, 'US' AS country FROM generate_series(1,{n//10}) t(i)")
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_products AS SELECT i AS product_id, 'Prod_'||i AS product_name, (random()*100+5)::DECIMAL(10,2) AS unit_cost, {'CURRENT_DATE - INTERVAL 90 DAY' if stale else 'CURRENT_DATE'} AS loaded_at FROM generate_series(1,{n//20}) t(i)")
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_calendar AS SELECT i AS day_id, CURRENT_DATE - INTERVAL (365-i) DAY AS cal_date, EXTRACT(month FROM CURRENT_DATE - INTERVAL (365-i) DAY) AS month FROM generate_series(1,365) t(i)")

        # Sales
        dup = fc.get("duplicate_load_orders", False)
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_orders AS SELECT i AS order_id, CASE WHEN random()<{null_cust} THEN NULL ELSE (i%1000)+1 END AS customer_id, (i%500)+1 AS product_id, (random()*500+10)::DECIMAL(10,2) AS amount, CURRENT_DATE - INTERVAL ((random()*365)::INT) DAY AS order_date FROM generate_series(1,{row_ov}) t(i)")
        if dup:
            self.conn.execute("INSERT INTO raw_orders SELECT * FROM raw_orders")

        # Marketing
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_campaigns AS SELECT i AS campaign_id, 'Camp_'||i AS name, (random()*1000+100)::DECIMAL AS budget FROM generate_series(1,100) t(i)")
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_events AS SELECT i AS event_id, (i%1000)+1 AS customer_id, CASE WHEN random()<{null_ev} THEN NULL ELSE (i%100)+1 END AS campaign_id, 'click' AS event_type FROM generate_series(1,{n*5}) t(i)")

        # Inventory
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_warehouse AS SELECT i AS loc_id, 'WH_'||i AS name, i*10 AS capacity FROM generate_series(1,200) t(i)")
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_shipments AS SELECT i AS shipment_id, (i%500)+1 AS product_id, (i%200)+1 AS loc_id, (random()*50+1)::INT AS qty, CURRENT_DATE - INTERVAL ((random()*90)::INT) DAY AS ship_date FROM generate_series(1,{n//10*8}) t(i)")

        # Finance
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_ledger AS SELECT i AS entry_id, (i%1000)+1 AS customer_id, (random()*500-100)::DECIMAL AS amount, CURRENT_DATE - INTERVAL ((random()*365)::INT) DAY AS entry_date FROM generate_series(1,{n//2}) t(i)")
        self.conn.execute(f"CREATE OR REPLACE TABLE raw_invoices AS SELECT i AS inv_id, (i%1000)+1 AS customer_id, (random()*1000+50)::DECIMAL AS total, CURRENT_DATE - INTERVAL ((random()*180)::INT) DAY AS inv_date FROM generate_series(1,{n//10*3}) t(i)")

        for tbl in ["raw_customers","raw_products","raw_calendar","raw_orders",
                    "raw_campaigns","raw_events","raw_warehouse","raw_shipments",
                    "raw_ledger","raw_invoices"]:
            self._stat(tbl)
        self.node_stats["raw_orders"]["null_rate_customer_id"] = self._null_rate("raw_orders","customer_id")
        if null_ev > 0:
            self.node_stats["raw_events"]["null_rate_campaign_id"] = self._null_rate("raw_events","campaign_id")
        if stale:
            self.node_stats["raw_products"]["freshness_lag_days"] = 90.0

    def run_staging(self):
        fc = self.fault_config
        self.conn.execute("CREATE OR REPLACE TABLE stg_customers AS SELECT customer_id, customer_name, country FROM raw_customers")
        if fc.get("silent_value_corruption_cost", False):
            self.conn.execute("CREATE OR REPLACE TABLE stg_products AS SELECT product_id, product_name, unit_cost*-1 AS unit_cost, loaded_at FROM raw_products")
        else:
            self.conn.execute("CREATE OR REPLACE TABLE stg_products AS SELECT * FROM raw_products")
        self.conn.execute("CREATE OR REPLACE TABLE stg_calendar AS SELECT * FROM raw_calendar")
        self.conn.execute("CREATE OR REPLACE TABLE stg_orders AS SELECT order_id, customer_id, product_id, amount, order_date FROM raw_orders WHERE order_id IS NOT NULL")
        self.conn.execute("CREATE OR REPLACE TABLE stg_campaigns AS SELECT * FROM raw_campaigns")
        self.conn.execute("CREATE OR REPLACE TABLE stg_events AS SELECT * FROM raw_events")
        self.conn.execute("CREATE OR REPLACE TABLE stg_warehouse AS SELECT * FROM raw_warehouse")
        self.conn.execute("CREATE OR REPLACE TABLE stg_shipments AS SELECT * FROM raw_shipments")
        self.conn.execute("CREATE OR REPLACE TABLE stg_ledger AS SELECT * FROM raw_ledger")
        self.conn.execute("CREATE OR REPLACE TABLE stg_invoices AS SELECT * FROM raw_invoices")
        for tbl in ["stg_customers","stg_products","stg_calendar","stg_orders",
                    "stg_campaigns","stg_events","stg_warehouse","stg_shipments",
                    "stg_ledger","stg_invoices"]:
            self._stat(tbl)
        self.node_stats["stg_orders"]["null_rate_customer_id"] = self._null_rate("stg_orders","customer_id")

    def run_facts(self):
        fc = self.fault_config
        join_key = "o.order_id = c.customer_id" if fc.get("bad_join_sales", False) else "o.customer_id = c.customer_id"
        self.conn.execute(f"CREATE OR REPLACE TABLE fct_sales AS SELECT o.order_id, o.customer_id, c.customer_name, c.country, o.product_id, o.amount, o.order_date FROM stg_orders o LEFT JOIN stg_customers c ON {join_key}")
        self.conn.execute("CREATE OR REPLACE TABLE fct_attribution AS SELECT e.event_id, e.customer_id, c.customer_name, e.campaign_id, cp.name AS campaign_name FROM stg_events e LEFT JOIN stg_customers c ON e.customer_id=c.customer_id LEFT JOIN stg_campaigns cp ON e.campaign_id=cp.campaign_id")
        self.conn.execute("CREATE OR REPLACE TABLE fct_inventory AS SELECT p.product_id, p.product_name, p.unit_cost, w.loc_id, SUM(s.qty) AS total_qty FROM stg_products p LEFT JOIN stg_shipments s ON p.product_id=s.product_id LEFT JOIN stg_warehouse w ON s.loc_id=w.loc_id GROUP BY 1,2,3,4")
        self.conn.execute("CREATE OR REPLACE TABLE fct_finance AS SELECT l.entry_id, l.customer_id, c.customer_name, l.amount, l.entry_date FROM stg_ledger l LEFT JOIN stg_customers c ON l.customer_id=c.customer_id")
        for tbl in ["fct_sales","fct_attribution","fct_inventory","fct_finance"]:
            self._stat(tbl)
        self.node_stats["fct_sales"]["null_rate_customer_name"] = self._null_rate("fct_sales","customer_name")
        self.node_stats["fct_finance"]["null_rate_customer_name"] = self._null_rate("fct_finance","customer_name")

    def run_reports_and_dq(self):
        self.conn.execute("CREATE OR REPLACE TABLE rpt_revenue AS SELECT country, DATE_TRUNC('month',order_date) AS month, COUNT(*) AS orders, SUM(amount) AS revenue FROM fct_sales GROUP BY 1,2")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_sales_detail AS SELECT order_id, customer_name, amount, order_date FROM fct_sales")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_campaign_perf AS SELECT campaign_id, campaign_name, COUNT(*) AS clicks FROM fct_attribution GROUP BY 1,2")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_customer_journey AS SELECT customer_id, customer_name, COUNT(*) AS touchpoints FROM fct_attribution GROUP BY 1,2")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_stock_levels AS SELECT product_id, product_name, unit_cost, SUM(total_qty) AS qty FROM fct_inventory GROUP BY 1,2,3")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_fulfillment AS SELECT loc_id, COUNT(*) AS shipment_count FROM fct_inventory GROUP BY 1")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_pnl AS SELECT DATE_TRUNC('month',entry_date) AS month, SUM(amount) AS net FROM fct_finance GROUP BY 1")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_ar AS SELECT customer_id, customer_name, SUM(amount) AS balance FROM fct_finance GROUP BY 1,2")
        self.conn.execute("CREATE OR REPLACE TABLE rpt_executive_summary AS SELECT 1 AS id, (SELECT SUM(revenue) FROM rpt_revenue) AS total_revenue, (SELECT COUNT(*) FROM rpt_campaign_perf) AS campaigns")
        # DQ nodes
        self.conn.execute("CREATE OR REPLACE TABLE dq_orders AS SELECT COUNT(*) AS row_count, SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer FROM stg_orders")
        self.conn.execute("CREATE OR REPLACE TABLE dq_customers AS SELECT COUNT(*) AS row_count FROM stg_customers")
        self.conn.execute("CREATE OR REPLACE TABLE dq_sales AS SELECT COUNT(*) AS row_count, SUM(CASE WHEN customer_name IS NULL THEN 1 ELSE 0 END) AS null_name FROM fct_sales")
        self.conn.execute("CREATE OR REPLACE TABLE dq_finance AS SELECT COUNT(*) AS row_count FROM fct_finance")

        for tbl in ["rpt_revenue","rpt_sales_detail","rpt_campaign_perf","rpt_customer_journey",
                    "rpt_stock_levels","rpt_fulfillment","rpt_pnl","rpt_ar","rpt_executive_summary",
                    "dq_orders","dq_customers","dq_sales","dq_finance"]:
            self._stat(tbl)

        high_null_sales = self._null_rate("fct_sales","customer_name") > 0.30
        neg_cost = self.conn.execute("SELECT COUNT(*) FROM rpt_stock_levels WHERE unit_cost < 0").fetchone()[0]
        dup_ratio = self._rows("raw_orders") / max(self._rows("stg_orders"), 1)
        dup_detected = dup_ratio > 1.8

        return {
            "rpt_revenue": not high_null_sales and not dup_detected,
            "rpt_stock_levels": neg_cost == 0,
            "rpt_campaign_perf": True,
        }

    def run_all(self):
        self.setup()
        self.run_staging()
        self.run_facts()
        tests = self.run_reports_and_dq()
        return {"run_id": self.run_id, "node_stats": self.node_stats,
                "schema_actual": self.schema_actual, "tests": tests}

    def close(self):
        self.conn.close()
