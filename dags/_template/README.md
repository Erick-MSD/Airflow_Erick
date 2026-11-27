# Plantilla para un DAG nuevo

1. Copia esta carpeta con el nombre del pipeline: `cp -r dags/_template dags/mi_pipeline`.
2. Renombra `example_dag.py` (por ejemplo `mi_pipeline_dag.py`) y cambia el `dag_id`.
3. Pon la lógica del ETL en `pipeline.py`, los CSV de entrada en `data/` y los scripts SQL en `sql/`.
4. Importa tu código con el nombre del paquete: `from mi_pipeline.pipeline import ...`.

La carpeta `_template` está en `dags/.airflowignore`, así que Airflow no la carga.
