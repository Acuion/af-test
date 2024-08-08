import jwt
import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

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

    now = int(time.time())
    payload = {
        'aud': 'https://auth.double.cloud/oauth/token',
        'iss': connection.login,
        'sub': connection.login,
        'iat': now,
        'exp': now + 360}

    # JWT generation.
    encoded_token = jwt.encode(
        payload,
        connection.password,
        algorithm='PS256',
        headers={'kid': connection.extra_dejson.get('kid')})
    print("kid is", connection.extra_dejson.get('kid'))
    print("encoded token is", encoded_token)

    resp = requests.post("https://auth.double.cloud/oauth/token", headers={"Content-Type": "application/x-www-form-urlencoded"}, data=f"grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={encoded_token}")
    print(f"Yay, I got IAM token with status {resp.status_code}: {resp.text}")


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
