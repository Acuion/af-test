import doublecloud
import base64
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

from doublecloud.kafka.v1.cluster_service_pb2_grpc import ClusterServiceStub


# Define the function that prints the connection details
def print_connection_info():
    # Define the connection ID derived from the environment variable name
    conn_id = 'doublecloud_api_private_key'

    # Fetch the connection using Airflow's connection management system
    connection = BaseHook.get_connection(conn_id)

    # Print connection details
    print(f"Connection ID: {connection.conn_id}")
    print(f"Host: {connection.host}")
    print(f"Schema: {connection.schema}")
    print(f"Login: {connection.login}")
    print(f"Password: {connection.password}")
    print(f"Port: {connection.port}")
    print(f"Extra: {connection.extra}")

    sa_key = {
        "id": connection.extra_dejson.get('kid'),
        "service_account_id": connection.login,
        "private_key": connection.password
    }
    sdk = doublecloud.SDK(service_account_key=sa_key)

    cluster_service = sdk.client(ClusterServiceStub)
    print(cluster_service.List())


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id='print_connection_info_dag',
    default_args=default_args,
    description='A simple DAG to print connection info using Airflow connection management',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the task using PythonOperator
    print_connection_info_task = PythonOperator(
        task_id='print_connection_info_task',
        python_callable=print_connection_info,
    )

# Set the task in the DAG
print_connection_info_task
