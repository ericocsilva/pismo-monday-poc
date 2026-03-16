"""
Pismo Monday.com PoC - Lakeflow Community Connector
====================================================
Entry point for Spark Declarative Pipeline (SDP) that ingests Monday.com
data (boards, items, users, groups) into Unity Catalog using the community
connector from:
  https://github.com/colin-gibbons/lakeflow-community-connectors/tree/monday-connector

Boards used in the demo:
  - PismoPoC (18403157289)
  - Pismo - Card Processing Sprint (18403163013)
  - Pismo - Incident & Bug Tracking (18403163137)
  - Pismo - Customer Onboarding Pipeline (18403163276)
"""

import sys

# Add the pipeline root to sys.path so that 'libs', 'pipeline', 'sources' are importable.
# In Databricks SDP pipelines, __file__ is not defined; use the known workspace path.
_root = "/Workspace/Users/erico.silva@databricks.com/pismo-monday-poc"
if _root not in sys.path:
    sys.path.insert(0, _root)

from pipeline.ingestion_pipeline import ingest
from libs.source_loader import get_register_function

source_name = "monday"

# API token stored in Databricks Secrets (scope: monday_poc, key: api_token)
api_token = dbutils.secrets.get(scope="monday_poc", key="api_token")  # noqa: F821

TARGET_CATALOG = "catalog_ajcos9_0aa1b0"
TARGET_SCHEMA = "pismo_monday_poc"

# Board IDs for all Pismo demo boards
BOARD_IDS = "18403157289,18403163013,18403163137,18403163276"

pipeline_spec = {
    "connection_name": "monday_pismo_poc",
    "objects": [
        {
            "table": {
                "source_table": "boards",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                    "board_ids": BOARD_IDS,
                },
            }
        },
        {
            "table": {
                "source_table": "items",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                    "board_ids": BOARD_IDS,
                },
            }
        },
        {
            "table": {
                "source_table": "users",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                },
            }
        },
        {
            "table": {
                "source_table": "groups",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                    "board_ids": BOARD_IDS,
                },
            }
        },
        {
            "table": {
                "source_table": "updates",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                    "board_ids": BOARD_IDS,
                },
            }
        },
        {
            "table": {
                "source_table": "activity_logs",
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,
                    "board_ids": BOARD_IDS,
                },
            }
        },
    ],
}

# Register the Monday.com Lakeflow Python source with Spark
register_lakeflow_source = get_register_function(source_name)
register_lakeflow_source(spark)  # noqa: F821

# Run the ingestion pipeline
ingest(spark, pipeline_spec)  # noqa: F821
