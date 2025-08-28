from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("print_date_dag",
         start_date=datetime(2025, 1, 1),
         schedule="@hourly",
         catchup=False) as dag:

    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )
