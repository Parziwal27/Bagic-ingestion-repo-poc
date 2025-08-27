from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def _ingest(source: str):
    print(f"Ingesting data from {source}")

with DAG("ingestion_demo_dag",
         start_date=datetime(2025, 1, 1),
         schedule="@daily",
         catchup=False,
         params={"source": "s3://example-bucket/input"}) as dag:

    ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=_ingest,
        op_args=["{{ params.source }}"]
    )
