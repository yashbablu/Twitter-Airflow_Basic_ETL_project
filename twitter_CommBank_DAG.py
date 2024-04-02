
#import necessary library and operators
from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from twitter_etl import run_twitter_etl

#define necessary aruguments
default_args = {
    'owner' : 'CommBank_airflow',
    'depends_on_past' : False, #because in this task we have only one DAG
    'start_date' : datetime.today(),
    'email_on_failure' : False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#define DAG(name, arguments, description)
dag = DAG(
    'twitter_dag',
    default_args = default_args,
    description = 'First ETL Code'
)

#define operator -- here we are using pythonOperator
run_etl = PythonOperator(
    task_id = 'complete_twitter_etl',
    python_callable = run_twitter_etl,
    dag=dag,
)

run_etl