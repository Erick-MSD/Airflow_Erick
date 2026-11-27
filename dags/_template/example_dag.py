from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Cambia "_template" por el nombre de la carpeta del nuevo pipeline
from _template.pipeline import extract, load, transform

dagId = "example_pipeline"  # nombre único del DAG en Airflow
startDate = datetime(2026, 1, 1)  # fecha desde la que puede correr
scheduleInterval = "@daily"  # frecuencia de ejecución


def extractTask():
    return extract()


def transformTask(**context):
    # Recupera el resultado del paso anterior desde XCom
    extractResult = context["ti"].xcom_pull(task_ids="extract")
    return transform(extractResult)


def loadTask(**context):
    transformResult = context["ti"].xcom_pull(task_ids="transform")
    load(transformResult)


with DAG(
    dag_id=dagId,
    start_date=startDate,
    schedule_interval=scheduleInterval,
    catchup=False,
    tags=["template"],
) as dag:
    extractStep = PythonOperator(task_id="extract", python_callable=extractTask)
    transformStep = PythonOperator(task_id="transform", python_callable=transformTask)
    loadStep = PythonOperator(task_id="load", python_callable=loadTask)

    extractStep >> transformStep >> loadStep
