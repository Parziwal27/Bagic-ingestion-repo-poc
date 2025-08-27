from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("hello_world_dag",
         start_date=datetime(2025, 1, 1),
         schedule="@daily",
         catchup=False) as dag:

    task = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello World from Airflow!'"
    )
