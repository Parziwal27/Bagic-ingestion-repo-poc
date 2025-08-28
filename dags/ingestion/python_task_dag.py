from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def _print_message():
    print("This is a Python task DAG.")


with DAG(
    "python_task_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_message",
        python_callable=_print_message,
    )

