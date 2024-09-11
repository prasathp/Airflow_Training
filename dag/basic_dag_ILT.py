from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import logging
from scripts.basic_dag_incremental_backup import incremental_etl


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# PostgreSQL connection parameters
POSTGRESQL_CONFIG = {
    "host": "telecom.c2b3frbss0vu.ap-south-1.rds.amazonaws.com",
    "user": "postgres",
    "password": "Telecom_Airflow123",
    "database": "wetelco_telecom",
    "port": 5432,
}

DESTINATION_BUCKET = "airflow-destination-data/burhan"

default_args = {"owner": "airflow", "start_date": datetime(2024, 8, 15)}

dag = DAG(
    dag_id="prasath_basic_dag_ILT", schedule_interval="0 0 * * 1,3,5", default_args=default_args
)

customer_information_task = PythonOperator(
    task_id=f"process_customer_information",
    python_callable=incremental_etl,
    op_args=["customer_information", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    dag=dag
)

customer_rating_task = PythonOperator(
    task_id=f"process_customer_rating",
    python_callable=incremental_etl,
    op_args=["customer_rating", DESTINATION_BUCKET, POSTGRESQL_CONFIG],
    dag=dag
)

# Define task dependencies
customer_information_task >> customer_rating_task
