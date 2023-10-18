from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Define default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'my_http_request_dag',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,
)

# Create an HTTP sensor to wait for the service to be available
http_sensor = HttpSensor(
    task_id='http_sensor_task',
    http_conn_id='http_default',  # Create an HTTP connection in Airflow with the base URL
    endpoint='/schedule.json',
    request_params={'project': 'bookscraper', 'spider': 'bookspider'},
    response_check=lambda response: True if response.status_code == 200 else False,
    timeout=120,  # Adjust the timeout as needed
    mode='poke',  # You can use 'reschedule' mode if desired
    poke_interval=30,  # Adjust the interval as needed
    dag=dag,
)

# Create an HTTP operator to send the POST request
http_operator = SimpleHttpOperator(
    task_id='http_request_task',
    method='POST',
    http_conn_id='http_default',  # Create an HTTP connection in Airflow with the base URL
    endpoint='/schedule.json',
    data={'project': 'bookscraper', 'spider': 'bookspider'},
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    xcom_push=True,
    dag=dag,
)

# You should set up an HTTP connection in Airflow with the base URL
# http://localhost:6800 in the Airflow UI.

# Define the task dependencies
http_sensor >> http_operator

if __name__ == '__main__':
    dag.cli()