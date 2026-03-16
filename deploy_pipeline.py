"""
Deploy Pismo Monday.com PoC Lakeflow pipeline to Databricks workspace.
Uploads all connector files and creates the Spark Declarative Pipeline.
"""
import os
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines, workspace

WORKSPACE_HOST = "https://adb-7405612655723059.19.azuredatabricks.net"
BASE_PATH = "/Users/erico.silva/Monday"

# Workspace path for all connector files
WS_BASE = "/Workspace/Users/erico.silva@databricks.com/pismo-monday-poc"

# All files to upload: (local_relative_path, workspace_relative_path)
FILES = [
    ("pismo_monday_ingest.py", "pismo_monday_ingest.py"),
    ("pipeline/__init__.py", "pipeline/__init__.py"),
    ("pipeline/ingestion_pipeline.py", "pipeline/ingestion_pipeline.py"),
    ("libs/__init__.py", "libs/__init__.py"),
    ("libs/spec_parser.py", "libs/spec_parser.py"),
    ("libs/source_loader.py", "libs/source_loader.py"),
    ("libs/utils.py", "libs/utils.py"),
    ("sources/__init__.py", "sources/__init__.py"),
    ("sources/interface/__init__.py", "sources/interface/__init__.py"),
    ("sources/interface/lakeflow_connect.py", "sources/interface/lakeflow_connect.py"),
    ("sources/monday/__init__.py", "sources/monday/__init__.py"),
    ("sources/monday/monday.py", "sources/monday/monday.py"),
    ("sources/monday/_generated_monday_python_source.py",
     "sources/monday/_generated_monday_python_source.py"),
]


def upload_file(ws: WorkspaceClient, local_path: str, ws_path: str):
    """Upload a file to the Databricks workspace using the SDK."""
    from databricks.sdk.service.workspace import ImportFormat
    full_local = os.path.join(BASE_PATH, local_path)
    full_ws = f"{WS_BASE}/{ws_path}"

    with open(full_local, "rb") as f:
        content = f.read()

    ws.workspace.import_(
        path=full_ws,
        content=base64.b64encode(content).decode("utf-8"),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    print(f"  Uploaded: {ws_path}")


def main():
    # Uses ~/.databrickscfg [TKO] profile for authentication
    ws = WorkspaceClient(host=WORKSPACE_HOST, profile="Demo")

    print("=== Uploading connector files to workspace ===")
    for local, remote in FILES:
        upload_file(ws, local, remote)

    print("\n=== Creating/updating Lakeflow pipeline ===")

    # Only the entry script is needed as a pipeline library.
    # The sys.path setup in the entry script handles all imports.
    all_libraries = [
        pipelines.PipelineLibrary(file=pipelines.FileLibrary(path=f"{WS_BASE}/pismo_monday_ingest.py")),
    ]

    PIPELINE_NAME = "Pismo - Monday.com Lakeflow Community Connector"

    # Check if pipeline already exists
    existing_id = None
    try:
        for p in ws.pipelines.list_pipelines():
            if p.name == PIPELINE_NAME:
                existing_id = p.pipeline_id
                break
    except Exception:
        pass

    create_kwargs = dict(
        name=PIPELINE_NAME,
        catalog="catalog_ajcos9_0aa1b0",
        schema="pismo_monday_poc",
        serverless=True,
        channel="PREVIEW",
        libraries=all_libraries,
        configuration={
            "spark.databricks.delta.preview.enabled": "true",
        },
        development=True,
    )

    if existing_id:
        print(f"  Pipeline exists (ID: {existing_id}), updating...")
        ws.pipelines.update(pipeline_id=existing_id, **create_kwargs)
        pipeline_id = existing_id
    else:
        response = ws.pipelines.create(**create_kwargs)
        pipeline_id = response.pipeline_id
        print(f"  Pipeline created (ID: {pipeline_id})")

    print(f"\n=== Pipeline deployed successfully ===")
    print(f"Pipeline ID: {pipeline_id}")
    print(f"Workspace: {WORKSPACE_HOST}")
    print(f"UI: {WORKSPACE_HOST}/#joblist/pipelines/{pipeline_id}")

    return pipeline_id


if __name__ == "__main__":
    main()
