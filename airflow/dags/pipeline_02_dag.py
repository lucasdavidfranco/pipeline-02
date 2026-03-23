from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="pipeline_02_sales_batch",
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "batch", "sales"]
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python -m spark_job.generate_data",
        cwd="/opt/airflow/project"
    )

    run_spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="python -m spark_job.main",
        cwd="/opt/airflow/project"
    )

    generate_data >> run_spark_job