from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow'
}

# Define the DAG
dag = DAG(
    'example_dag_prasath',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='0 10 * * 1-5',
    start_date=days_ago(1),
    catchup=False,
)

# Define a Python function to be used in a PythonOperator
def print_hello():
    print('Hello, Airflow!')

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set the task dependencies
start >> hello_task >> end