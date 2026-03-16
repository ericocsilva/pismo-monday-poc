# Pismo Monday.com → Databricks Unity Catalog PoC

This repository contains the **Lakeflow Community Connector for Monday.com**, adapted and deployed as a Proof of Concept for Pismo. It ingests data from Monday.com boards into Databricks Unity Catalog using a **Spark Declarative Pipeline (LakeFlow)**.

---

## Overview

The connector reads data from Monday.com's GraphQL API and lands it as Delta tables in Unity Catalog. The pipeline runs on Databricks Serverless Spark Declarative Pipelines (preview channel) and supports both **CDC (incremental)** and **snapshot (full refresh)** ingestion modes.

**Upstream connector repo:**
https://github.com/colin-gibbons/lakeflow-community-connectors/tree/monday-connector

**Deployed workspace:**
https://adb-7405612655723059.19.azuredatabricks.net
Pipeline: `Pismo - Monday.com Lakeflow Community Connector` (ID: `21c9662d-776f-4f9e-97e7-2870c062b899`)

---

## What Was Built

### Architecture

```
Monday.com GraphQL API
        │
        ▼
Custom Spark DataSource ("lakeflow_connect" format)
  sources/monday/_generated_monday_python_source.py
  sources/monday/monday.py
        │
        ▼
Spark Declarative Pipeline (SDP)
  pismo_monday_ingest.py  ←  entry point
  pipeline/ingestion_pipeline.py
        │
        ▼
Unity Catalog — catalog_ajcos9_0aa1b0.pismo_monday_poc
  ├── boards          (CDC / incremental)
  ├── items           (CDC / incremental)
  ├── users           (snapshot / full refresh)
  ├── groups          (snapshot / full refresh)
  ├── updates         (snapshot / full refresh)
  └── activity_logs   (snapshot / full refresh)
```

### Ingested Tables

| Table | Rows (demo) | Sync Mode | Description |
|---|---|---|---|
| `boards` | 4 | CDC | Board metadata (name, state, workspace, item count) |
| `items` | 27 | CDC | Tasks/rows on each board, including all column values as JSON |
| `users` | 1 | Snapshot | Account users with profile information |
| `groups` | 5 | Snapshot | Sections/groups within boards |
| `updates` | — | Snapshot | Comments and threads on items |
| `activity_logs` | 57 | Snapshot | Full audit trail of all board and item changes |

### CDC via Activity Logs

For `boards` and `items`, Monday.com's GraphQL API does not support filtering by `updated_at`. The connector uses the **Activity Logs API** as a change feed:

1. **First run**: full snapshot of all records.
2. **Subsequent runs**: queries activity logs since the last sync timestamp (with a 60-second lookback window) to identify changed entities, then fetches only those records.

### Key Fix Applied

During the `_get_table_metadata` phase of the pipeline, the Spark DataSource receives only a consolidated `tableConfigs` JSON blob — the `api_token` is not passed as a top-level option. A fallback was added to `_generated_monday_python_source.py` to extract the token from `tableConfigs` when it is not present directly:

```python
api_token = options.get("api_token")
if not api_token:
    import json as _json
    try:
        _tcs = _json.loads(options.get("tableConfigs", "{}"))
        for _tc in _tcs.values():
            if isinstance(_tc, dict) and _tc.get("api_token"):
                api_token = _tc["api_token"]
                break
    except Exception:
        pass
```

---

## Repository Structure

```
Monday/
├── pismo_monday_ingest.py              # SDP pipeline entry point
├── deploy_pipeline.py                  # Deployment script (uploads files + creates pipeline)
├── databricks.yml                      # Databricks Asset Bundle (DAB) definition
├── pyproject.toml
│
├── pipeline/
│   ├── ingestion_pipeline.py           # Core ingest logic: CDC / snapshot / append flows
│   └── lakeflow_python_source.py
│
├── libs/
│   ├── spec_parser.py                  # Pydantic parser for pipeline_spec dict
│   ├── source_loader.py                # Dynamically loads the connector's register function
│   └── utils.py
│
└── sources/
    ├── interface/
    │   └── lakeflow_connect.py         # Base interface all connectors implement
    └── monday/
        ├── monday.py                   # Monday.com GraphQL API client + table logic
        ├── _generated_monday_python_source.py  # Auto-generated Spark DataSource wrapper
        ├── connector_spec.yaml         # Table schemas and sync mode definitions
        └── README.md                   # Monday.com connector reference docs
```

The `sources/` directory also includes other community connectors (GitHub, HubSpot, Mixpanel, Stripe, Zendesk) that ship in the upstream repo but are not used in this PoC.

---

## Prerequisites

### Monday.com

- A Monday.com account (the demo uses `databricks792224.monday.com`)
- A personal API token — see [Obtaining a Monday.com API Token](#obtaining-a-mondaycom-api-token)
- Access to the boards you want to ingest

### Databricks

- A Databricks workspace with Unity Catalog enabled
- Serverless compute available (pipeline uses `serverless: true`)
- A Databricks Secrets scope to store the API token securely
- Databricks CLI authenticated (for `deploy_pipeline.py`)

---

## Setup

### 1. Clone this repository

```bash
git clone https://github.com/erico-silva_data/pismo-monday-poc.git
cd pismo-monday-poc
```

### 2. Install local dependencies (for local development only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 3. Obtain a Monday.com API Token

1. Log in to your Monday.com account
2. Click your profile picture (bottom-left corner)
3. Select **Developers**
4. Click **My Access Tokens** tab
5. Click **Show** to reveal your personal API token

### 4. Store the API token in Databricks Secrets

```bash
# Create a secret scope (only needs to be done once)
databricks secrets create-scope monday_poc

# Store the API token
databricks secrets put-secret monday_poc api_token --string-value "<YOUR_MONDAY_API_TOKEN>"
```

The pipeline reads the token at runtime with:
```python
api_token = dbutils.secrets.get(scope="monday_poc", key="api_token")
```

### 5. Configure `pismo_monday_ingest.py`

Open [pismo_monday_ingest.py](pismo_monday_ingest.py) and update the following constants to match your environment:

```python
_root = "/Workspace/Users/<your-user>@databricks.com/pismo-monday-poc"

TARGET_CATALOG = "<your-unity-catalog>"
TARGET_SCHEMA  = "<your-schema>"

BOARD_IDS = "18403157289,18403163013,18403163137,18403163276"
# Replace with your own board IDs from Monday.com
```

> **Why is `_root` hardcoded?**
> Databricks Spark Declarative Pipelines run notebook-style — `__file__` is not defined. The root path must be set explicitly so Python can resolve `libs`, `pipeline`, and `sources` imports.

---

## Deploying the Pipeline

### Option A — `deploy_pipeline.py` (recommended for first-time setup)

This script uploads all connector files to the Databricks workspace and creates (or updates) the SDP pipeline via the SDK.

```bash
# Ensure ~/.databrickscfg has a [Demo] profile pointing to your workspace:
# [Demo]
# host      = https://adb-7405612655723059.19.azuredatabricks.net
# auth_type = databricks-cli

DATABRICKS_CONFIG_PROFILE=Demo python deploy_pipeline.py
```

The script will:
1. Upload all Python source files to `/Workspace/Users/<user>/pismo-monday-poc/`
2. Check if the pipeline `"Pismo - Monday.com Lakeflow Community Connector"` already exists
3. Create it (or update it) with serverless compute, PREVIEW channel, and the entry point library

### Option B — Databricks Asset Bundle (DAB)

```bash
databricks bundle deploy --target dev
```

The [databricks.yml](databricks.yml) defines all library files and pipeline settings. Authentication uses your active `databricks` CLI profile.

### Option C — Manual workspace upload

Upload all files in the repository to a workspace folder and create a pipeline manually via the Databricks UI, pointing to `pismo_monday_ingest.py` as the library.

---

## Running the Pipeline

Once deployed, trigger a pipeline run from the Databricks UI:

1. Navigate to **Workflows → Delta Live Tables** (or the Pipelines section)
2. Find `"Pismo - Monday.com Lakeflow Community Connector"`
3. Click **Start** (full refresh) or **Trigger** (incremental)

Or via CLI:

```bash
databricks pipelines start 21c9662d-776f-4f9e-97e7-2870c062b899 --profile=Demo
```

### Verifying the Data

After the pipeline completes, query the tables in SQL:

```sql
USE CATALOG catalog_ajcos9_0aa1b0;
USE SCHEMA pismo_monday_poc;

SELECT * FROM boards;
SELECT * FROM items LIMIT 10;
SELECT COUNT(*) FROM activity_logs;
```

---

## Pipeline Spec Reference

The `pipeline_spec` dictionary in [pismo_monday_ingest.py](pismo_monday_ingest.py) controls which tables are ingested and where they land:

```python
pipeline_spec = {
    "connection_name": "monday_pismo_poc",   # logical name for the connection
    "objects": [
        {
            "table": {
                "source_table": "items",             # Monday.com object to fetch
                "destination_catalog": TARGET_CATALOG,
                "destination_schema": TARGET_SCHEMA,
                "table_configuration": {
                    "api_token": api_token,           # passed to the DataSource
                    "board_ids": BOARD_IDS,           # optional: filter by board
                },
            }
        },
        # ... one entry per table
    ],
}
```

### Supported `table_configuration` Options

| Option | Tables | Description |
|---|---|---|
| `api_token` | all | Monday.com personal API token (required) |
| `board_ids` | boards, items, groups, updates, activity_logs | Comma-separated board IDs to scope the sync |
| `page_size` | boards, items, users | Records per API page (default: 50 for boards/users, 100 for items) |
| `state` | boards | Filter by state: `active`, `all`, `archived`, `deleted` (default: `all`) |
| `kind` | users | Filter users: `all`, `guests`, `non_guests`, `non_pending` (default: `all`) |

---

## Demo Boards (Pismo PoC)

| Board | ID |
|---|---|
| PismoPoC | 18403157289 |
| Pismo - Card Processing Sprint | 18403163013 |
| Pismo - Incident & Bug Tracking | 18403163137 |
| Pismo - Customer Onboarding Pipeline | 18403163276 |

These boards live in the `databricks792224.monday.com` demo environment and were populated with sample sprint tasks, bug reports, and onboarding entries to simulate a real Pismo operational context.

---

## Security Notes

- The Monday.com API token is **never stored in code**. It is retrieved at runtime from Databricks Secrets (`scope=monday_poc`, `key=api_token`).
- `deploy_pipeline.py` uses profile-based authentication (`~/.databrickscfg`) — no tokens in source files.
- `sources/monday/configs/dev_config.json` (used for local development only) is excluded from git via `.gitignore`.

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `NameError: name '__file__' is not defined` | SDP pipelines run as notebook cells | Ensure `_root` is hardcoded in the entry script |
| `ModuleNotFoundError: No module named 'libs'` | `_root` not in `sys.path` | Verify `sys.path.insert(0, _root)` runs before imports |
| `ValueError: Monday.com connector requires 'api_token'` | Token missing during metadata read phase | The `tableConfigs` fallback in `_generated_monday_python_source.py` handles this |
| `401 Unauthorized` from Monday.com API | Token expired or incorrect | Regenerate token and update Databricks Secret |
| `BadRequest: unsupported first path component: Workspace` | Using `ws.files.upload()` for workspace paths | Use `ws.workspace.import_()` SDK method (already done in `deploy_pipeline.py`) |

---

## References

- [Lakeflow Community Connectors upstream repo](https://github.com/colin-gibbons/lakeflow-community-connectors/tree/monday-connector)
- [Monday.com API documentation](https://developer.monday.com/api-reference/docs)
- [Monday.com Authentication Guide](https://developer.monday.com/api-reference/docs/authentication)
- [Databricks Spark Declarative Pipelines](https://docs.databricks.com/aws/en/delta-live-tables/index.html)
- [Databricks Secrets](https://docs.databricks.com/security/secrets/index.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
