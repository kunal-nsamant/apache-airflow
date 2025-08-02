import pendulum
import logging
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

# Import the refactored functions from your Python scripts
from salon_pipeline.mysql_to_azuresql_extractor import run_mysql_extraction
from salon_pipeline.azuresql_to_snowflake_loader import run_azuresql_to_snowflake_load
from salon_pipeline.azuresql_transform import run_azuresql_merge

# --- Define the DAG ---
@dag(
    dag_id="salon_data_pipeline_dag",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "schedule_interval": pendulum.duration(minutes=30),
        "start_date": pendulum.datetime(2025, 7, 1, tz="UTC"),
    },
    description="An end-to-end data pipeline to sync MySQL data to Snowflake using CDC.",
    catchup=False,
    tags=["data_pipeline", "mysql", "snowflake", "cdc"],
)
def salon_data_pipeline():
    """
    This DAG orchestrates the extraction, transformation, and loading of salon data.
    """
    
    # --- Task 1: Extract CDC data from MySQL and load to Azure SQL DB staging ---
    extract_and_stage_task = PythonOperator(
        task_id="extract_from_mysql_and_stage",
        python_callable=run_mysql_extraction,
    )

    # --- Task 2: Transform raw data in Azure SQL DB using a SQL script via Python ---
    transform_task = PythonOperator(
        task_id="transform_in_azuresql",
        python_callable=run_azuresql_merge,
    )
    
    # --- Task 3: Load the transformed data from Azure SQL DB to Snowflake ---
    load_to_snowflake_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=run_azuresql_to_snowflake_load,
    )

    # Define the task dependencies
    extract_and_stage_task >> transform_task >> load_to_snowflake_task

salon_data_pipeline()