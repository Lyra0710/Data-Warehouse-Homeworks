
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'ananya',
    'email': ['ananya.yallapragada@sjsu.edu'],
    
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='HelloWorld_docker',
    start_date=datetime(2026, 3, 8),
    catchup=False,
    tags=['example'],
    schedule='0 2 * * *',
    default_args=default_args
) as dag:

    run_hello = DockerOperator(
        task_id="run_hello",
        image="hello-image:latest",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        force_pull=False,
    )
    run_goodbye = DockerOperator(
        task_id="run_goodbye",
        image="goodbye-image:latest",
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        force_pull=False,
    )

    run_hello >> run_goodbye