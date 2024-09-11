from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pendulum
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import io
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024, 8, 26, tz="Asia/Kolkata"),
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "catchup": True,
    "email": ["burhanuddin@mentorskool.com", "amit@mentorskool.com"],
    "email_on_retry": True,
    "email_on_failure": True,
}


POSTGRES_CONN_ID = "telecom_airflow"
DESTINATION_BUCKET = "tredence-backup-bucket"
AGGREGATED_VIEW_BUCKET = "tredence-aggregated-view-bucket"


def fetch_and_update_last_run_date(hook, table: str):
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Check if a record exists
    sql_check = (
        "SELECT last_run_date FROM etl_last_run_metadata_burhan WHERE table_name = %s;"
    )
    result = hook.get_first(sql_check, parameters=(table,))

    if result:
        # Record exists, perform UPDATE
        sql_update = """
            UPDATE etl_last_run_metadata_burhan
            SET last_run_date = %s
            WHERE table_name = %s;
        """
        last_run_date = result[0]
        hook.run(sql_update, parameters=(current_date, table))
    else:
        # Record does not exist, perform INSERT
        sql_insert = """
            INSERT INTO etl_last_run_metadata_burhan (table_name, last_run_date)
            VALUES (%s, %s);
        """
        last_run_date = datetime(1970, 1, 1)
        hook.run(sql_insert, parameters=(table, current_date))

    return last_run_date


def fetch_data_and_upload_to_s3(table, **kwargs):
    ti = kwargs["ti"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) # connection with postgres

    # Fetch the last run date
    last_run_date = fetch_and_update_last_run_date(hook, table)

    # Fetch data from RDS table
    sql = f"SELECT * FROM {table} WHERE created_at > '{last_run_date}' or updated_at > '{last_run_date}';"
    df = hook.get_pandas_df(sql)

    if df.empty:
        # Push a message to XCom if no new data
        ti.xcom_push(key=f"s3_data_path", value="no new data")
        print(f"No new data to fetch from {table}.")
        return

    # Convert DataFrame to CSV
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = (
        f"prasath/{table}/{table}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    )

    s3_hook.load_string(
        string_data=csv_content,
        key=s3_key,
        bucket_name=DESTINATION_BUCKET,
        replace=True,
    )
    print(f"Uploaded data from {table} to s3://{DESTINATION_BUCKET}/{s3_key}")

    # push the s3 hook to xcom, so that we can perform transformation on that
    ti.xcom_push(key=f"s3_data_path", value=s3_key)


# Updated function to include the S3 key and to avoid storing the run date
def customer_rating_aggregated_view(**kwargs):
    try:
        ti = kwargs.get("ti")
        s3_hook = S3Hook(aws_conn_id="aws_default")

        # Fetch s3 key from xcom
        customer_rating_df_key = ti.xcom_pull(
            task_ids=f"get_last_run_date_group.get_customer_rating_last_run_date",
            key="s3_data_path",
        )

        customer_information_df_key = ti.xcom_pull(
            task_ids=f"get_last_run_date_group.get_customer_information_last_run_date",
            key="s3_data_path",
        )
        if (
            customer_rating_df_key == "no new data"
            or customer_information_df_key == "no new data"
        ):
            # No new data for customer_rating, so no analysis performed
            ti.xcom_push(key="aggregated_data_s3_path", value="no analysis performed")
            logger.info("No new data for customer_rating or customer_information. No analysis performed.")
            return

        # fetch the file from s3 in dataframe
        customer_rating_df = pd.read_csv(
            s3_hook.get_key(
                customer_rating_df_key, bucket_name=DESTINATION_BUCKET
            ).get()["Body"]
        )  # Fetch customer_rating_file

        customer_rating_df["updated_at"] = pd.to_datetime(
            customer_rating_df["updated_at"]
        )
        customer_rating_df = customer_rating_df.drop_duplicates(keep="last")

        # fetch the file from s3 in dataframe
        customer_information_df = pd.read_csv(
            s3_hook.get_key(
                customer_information_df_key, bucket_name=DESTINATION_BUCKET
            ).get()["Body"]
        )  # fetch customer_information file
        customer_information_df["updated_at"] = pd.to_datetime(
            customer_information_df["updated_at"]
        )
        customer_information_df = customer_information_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["customer_id"], keep="last")

        # Merge customer rating with customer information
        rating_info = pd.merge(
            customer_rating_df, customer_information_df, on="customer_id"
        )

        # Calculate average rating by connection type
        avg_rating_by_connection = (
            rating_info.groupby("connection_type")["rating"].mean().reset_index()
        )
        avg_rating_by_connection.columns = ["Connection Type", "Average Rating"]

        logger.info(avg_rating_by_connection.head(10))

        # Upload results to S3
        current_date = datetime.now()  # Define the current date

        # Upload results to S3
        current_date = datetime.now()
        output_key = f"burhan/customer_rating_analyses/customer_rating_analyses_{current_date.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        s3_hook.load_string(
            string_data=avg_rating_by_connection.to_csv(index=False),
            key=output_key,
            bucket_name=AGGREGATED_VIEW_BUCKET,
            replace=True,
        )
        logger.info(
            f"Uploaded aggregated view to s3://{AGGREGATED_VIEW_BUCKET}/{output_key}"
        )

        # push the s3 hook to xcom, so that we can perform transformation on that
        ti.xcom_push(key=f"aggregated_data_s3_path", value=output_key)
    except Exception as error:
        logger.error(error)
        raise


with DAG(
    dag_id="prasath_etl_dag_hooks_and_xcoms",
    start_date=days_ago(1),
    owner_links={"airflow": "https://airflow.apache.org/"},
    tags=["hooks", "xcoms"],
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
):
    with TaskGroup("get_last_run_date_group") as get_last_run_date_group:
        get_last_run_date_tasks = [
            PythonOperator(
                task_id=f"get_{table}_last_run_date",
                python_callable=fetch_data_and_upload_to_s3,
                op_args=[table],
            )
            for table in [
                "customer_information",
                "billing",
                "customer_rating",
                "device_information",
                "plans",
            ]
        ]

        # dependency between tasks
        get_last_run_date_tasks[0] >> get_last_run_date_tasks[1:-1]

    customer_rating_aggregated_view_task = PythonOperator(
        task_id=f"customer_rating_aggregated_view",
        python_callable=customer_rating_aggregated_view,
    )

    get_last_run_date_group >> customer_rating_aggregated_view_task