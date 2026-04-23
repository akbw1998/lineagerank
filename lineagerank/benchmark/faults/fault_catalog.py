"""
FaultPipe fault catalog — 8 fault types covering document-specified categories.

Fault types from: Foidl & Felderer (arXiv 2309.07067, JSS 2024) taxonomy +
                  gaps identified in the research document (schema drift,
                  null explosions, late partitions, duplicate loads, bad joins).
"""
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class Fault:
    id: str
    type: str
    description: str
    config: Dict
    ground_truth: str
    expected_failures: List[str]
    propagation_hops: int
    severity: str


FAULT_CATALOG = [
    Fault("F1", "null_flood",
          "80% of customer_id values in raw_orders set to NULL.",
          {"null_flood_customer_id": 0.80},
          "raw_orders", ["rpt_revenue"], 3, "high"),

    Fault("F2", "schema_drift",
          "raw_customers email column removed (schema breaking change).",
          {"schema_drift_customers": True},
          "raw_customers", ["stg_customers"], 1, "medium"),

    Fault("F3", "stale_partition",
          "raw_products loaded_at frozen 90 days in the past.",
          {"stale_partition_products": True},
          "raw_products", ["rpt_stock"], 3, "medium"),

    Fault("F4", "silent_value_corruption",
          "unit_cost sign-flipped in stg_products transformation.",
          {"silent_value_corruption_cost": True},
          "stg_products", ["rpt_stock"], 2, "high"),

    Fault("F5", "row_drop",
          "raw_orders volume reduced 80% (source outage simulation).",
          {"_row_scale_override": 2000},
          "raw_orders", ["rpt_revenue", "rpt_stock"], 3, "high"),

    Fault("F6", "duplicate_load",
          "raw_orders duplicated (double-loaded with no dedup).",
          {"duplicate_load_orders": True},
          "raw_orders", ["rpt_revenue", "rpt_stock"], 3, "high"),

    Fault("F7", "bad_join",
          "fct_sales join uses wrong key (product_id instead of customer_id).",
          {"bad_join_sales": True},
          "fct_sales", ["rpt_revenue"], 1, "high"),

    Fault("F8", "compound",
          "null_flood in raw_orders + stale_partition in raw_products.",
          {"null_flood_customer_id": 0.50, "stale_partition_products": True},
          "raw_orders", ["rpt_revenue", "rpt_stock"], 3, "high"),
]
