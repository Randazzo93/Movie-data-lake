from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'email': ['trandazzo93@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_1', default_args=default_args, schedule_interval='30 7 * * *')

t1 = BashOperator(
    task_id='movie_table-to_snowflake',
    bash_command='python3 ~/repos/movie-data-lake/scripts/Movies.py',
    dag=dag)