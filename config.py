"""
config.py
Centralized configuration for the E-Commerce Analytics Platform.

Single source of truth for catalog/schema/table names, storage paths,
and environment-specific settings so notebooks, jobs, DLT pipelines,
and tests never hardcode strings that can drift out of sync.

Usage:
    from config import Config
    cfg = Config(env="dev")          # or env="prod"
    df = spark.table(cfg.silver("events"))
"""

from dataclasses import dataclass, field
from typing import Dict


@dataclass(frozen=True)
class Config:
    env: str = "dev"  # "dev" | "prod" — mirrors DABs bundle targets

    # ---------------------------------------------------------------
    # Unity Catalog
    # ---------------------------------------------------------------
    catalog: str = field(init=False)
    bronze_schema: str = "bronze_layer"
    silver_schema: str = "silver_layer"
    gold_schema: str = "gold_layer"

    # ---------------------------------------------------------------
    # ADLS Gen2 / External Locations
    # ---------------------------------------------------------------
    storage_account: str = "ecommdatasy2026"
    raw_container: str = "raw-data"
    checkpoints_container: str = "checkpoints"
    quarantine_container: str = "quarantine"

    external_location_raw: str = "ecommerce_raw"
    external_location_checkpoints: str = "ecommerce_checkpoints"
    external_location_quarantine: str = "ecommerce_quarantine"

    # ---------------------------------------------------------------
    # Security
    # ---------------------------------------------------------------
    service_principal_name: str = "ecommerce-pipeline-spn"

    def __post_init__(self):
        catalog_by_env: Dict[str, str] = {
            "dev": "ecommerce_analytics_platform_dev",
            "prod": "ecommerce_analytics_platform_prod",
        }
        object.__setattr__(
            self, "catalog", catalog_by_env.get(self.env, catalog_by_env["dev"])
        )

    # ---------------------------------------------------------------
    # Fully-qualified table helpers
    # ---------------------------------------------------------------
    def bronze(self, table: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{table}"

    def silver(self, table: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table}"

    def gold(self, table: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{table}"

    # ---------------------------------------------------------------
    # ABFSS path helpers
    # ---------------------------------------------------------------
    def abfss_path(self, container: str) -> str:
        return (
            f"abfss://{container}@{self.storage_account}.dfs.core.windows.net/"
        )

    @property
    def raw_path(self) -> str:
        return self.abfss_path(self.raw_container)

    @property
    def checkpoints_path(self) -> str:
        return self.abfss_path(self.checkpoints_container)

    @property
    def quarantine_path(self) -> str:
        return self.abfss_path(self.quarantine_container)

    # ---------------------------------------------------------------
    # Known gold-layer tables (used by dashboard + tests)
    # ---------------------------------------------------------------
    GOLD_TABLES = (
        "customer_metrics",
        "product_performance",
        "kpi_summary",
        "top_products",
        "revenue_by_category",
    )

    BRONZE_TABLES = ("events",)
    SILVER_TABLES = ("events_clean",)

    # Data-quality expectations reused across DLT pipeline + tests
    REQUIRED_EVENT_COLUMNS = ("event_type", "product_id", "price", "user_id")
    VALID_EVENT_TYPES = ("view", "cart", "purchase", "remove_from_cart")

    # ---------------------------------------------------------------
    # Dashboard-facing gold table schemas (confirmed via Lakeview UI)
    # ---------------------------------------------------------------
    KPI_SUMMARY_COLUMNS = (
        "total_revenue",
        "total_customers",
        "total_purchases",
        "total_cart_adds",
        "total_views",
    )

    TOP_PRODUCTS_COLUMNS = (
        "product_id",
        "total_revenue",
        "units_sold",
        "category_code",
    )

    REVENUE_BY_CATEGORY_COLUMNS = (
        "category_code",
        "category_revenue",
        "category_units",
    )


def get_config(env: str = "dev") -> Config:
    """Convenience factory, e.g. get_config(spark.conf.get('bundle.target'))."""
    return Config(env=env)