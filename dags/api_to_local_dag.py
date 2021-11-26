# Python Standard Modules to test DAG and airflow
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # Start on 8th of Nov, 2021
    #'start_date': days_ago(0,0,0,0,0),
    'start_date': datetime(2021, 11, 24),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # In case of errors, do one retry
    'retries': 1,
    # Do the retry with 30 seconds delay after the error
    'retry_delay': timedelta(seconds=180),
    # Run once every 15 minutes
    'schedule_interval': '0 2 * * *'
}


# define the DAG
dag = DAG(
    dag_id='api_to_local',
    default_args=default_args,
    description='Extract data from API, and transfer it locally',
    schedule_interval='0 12 * * *',
    tags=['my_dags'],
)

t1 = BashOperator(
    task_id='create_file',
    bash_command='python /opt/airflow/dags/scripts/api_to_local.py',
    dag=dag
)

t2 = BashOperator(
    task_id='update_AWS_RDS_database',
    bash_command='python /opt/airflow/dags/scripts/local_to_database.py',
    dag=dag
    )
# task pipeline
t1>>t2