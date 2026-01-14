from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="test",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
):
    pass