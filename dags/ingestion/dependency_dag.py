from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("dependency_dag",
         start_date=datetime(2025, 1, 1),
         schedule="@daily",
         catchup=False) as dag:

    t1 = BashOperator(
        task_id="first",
        bash_command="echo 'First task'"
    )

    t2 = BashOperator(
        task_id="second",
        bash_command="echo 'Second task, runs after first'"
    )

    t1 >> t2
