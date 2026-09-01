from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from etl_pipeline import (
    OUTPUT_DIR,
    check_input_directory,
    extract_raw_files,
    load_to_postgres,
    save_transformed_outputs,
    transform_and_aggregate,
)


def _extract_task(**context):
    result = extract_raw_files()
    run_id = context["run_id"].replace(":", "_").replace("/", "_")
    staging_dir = OUTPUT_DIR / "airflow_staging" / run_id
    staging_dir.mkdir(parents=True, exist_ok=True)
    raw_paths = {}
    for name, frame in result.items():
        normalized_name = name.replace("-", "_")
        path = staging_dir / f"{normalized_name}.pickle"
        frame.to_pickle(path)
        raw_paths[normalized_name] = str(path)
    return raw_paths


def _transform_task(**context):
    raw_paths = context["ti"].xcom_pull(task_ids="extract_raw_files", key="return_value")
    raw_frames = {
        name.replace("-", "_"): pd.read_pickle(path)
        for name, path in raw_paths.items()
    }
    transformed_frames, audit_rows, batch_id = transform_and_aggregate(raw_frames)
    staging_dir = Path(next(iter(raw_paths.values()))).parent
    transformed_paths = {}
    for name, frame in transformed_frames.items():
        path = staging_dir / f"{name}.pickle"
        frame.to_pickle(path)
        transformed_paths[name] = str(path)
    audit_path = staging_dir / "audit_rows.json"
    audit_path.write_text(json.dumps(audit_rows, ensure_ascii=False), encoding="utf-8")
    return {"transformed_paths": transformed_paths, "audit_path": str(audit_path), "batch_id": batch_id}


def _save_task(**context):
    payload = context["ti"].xcom_pull(task_ids="transform_and_validate", key="return_value")
    transformed_frames = {
        name: pd.read_pickle(path)
        for name, path in payload["transformed_paths"].items()
    }
    audit_rows = json.loads(Path(payload["audit_path"]).read_text(encoding="utf-8"))
    batch_id = payload["batch_id"]
    output_dir = save_transformed_outputs(transformed_frames, audit_rows, batch_id)
    context["ti"].xcom_push(key="output_dir", value=output_dir)
    return output_dir


def _load_task(**context):
    payload = context["ti"].xcom_pull(task_ids="transform_and_validate", key="return_value")
    transformed_frames = {
        name: pd.read_pickle(path)
        for name, path in payload["transformed_paths"].items()
    }
    audit_rows = json.loads(Path(payload["audit_path"]).read_text(encoding="utf-8"))
    batch_id = payload["batch_id"]
    load_to_postgres(transformed_frames, audit_rows, batch_id)
    return {"status": "loaded", "batch_id": batch_id}


with DAG(
    dag_id="northwind_postgres_pipeline",
    description="Northwind ETL pipeline using Airflow and PostgreSQL",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["northwind", "postgres", "tableau"],
) as dag:

    start = EmptyOperator(task_id="start")

    validate_input = PythonOperator(
        task_id="check_input_directory",
        python_callable=check_input_directory,
    )

    extract_raw = PythonOperator(
        task_id="extract_raw_files",
        python_callable=_extract_task,
    )

    transform_and_validate = PythonOperator(
        task_id="transform_and_validate",
        python_callable=_transform_task,
    )

    save_artifacts = PythonOperator(
        task_id="save_output_files",
        python_callable=_save_task,
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=_load_task,
    )

    start >> validate_input >> extract_raw >> transform_and_validate >> save_artifacts >> load_postgres
