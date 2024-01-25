import pprint as pp
import time
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.python import PythonOperator,BranchPythonOperator,PythonVirtualenvOperator
from datetime import datetime, timedelta
from requests.auth import HTTPBasicAuth,AuthBase
import requests
import logging

# Fivetran connector id, you will get it from Fivetran UI
src_connector_id = 'albatross_eternally'
# Fivetran API keys, you will get it from Fivetran UI
api_key = 'ZBMkIIu8IPJeRaii'
api_secret = '67FudOF9gStYmqQVgJH87IUsHBRP2vEp'

a = HTTPBasicAuth(api_key, api_secret)
base_url = 'https://api.fivetran.com/v1'
h = {
    'Authorization': f'Bearer {api_key}:{api_secret}'
}

activate_src_conn_params = ['PATCH', 'connectors/' + src_connector_id, {"paused": False}]
pause_src_conn_params = ['PATCH', 'connectors/' + src_connector_id, {"paused": True}]
sync_src_conn_params = ['POST', 'connectors/'+ src_connector_id+'/sync', {"force": True}]
src_status_check_conn_params = [src_connector_id,'pause_source_connector','none']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}   

def atlas(method, endpoint, payload=None):
    logging.info("Makiing request to pause or unpause connector")
    url = f'{base_url}/{endpoint}'
    logging.info(url)
    try:
        if method == 'PATCH':
            response = requests.patch(url, headers=h, json=payload, auth=a)
        else:
            raise ValueError('Invalid request method.')
        response.raise_for_status()  # Raise exception
        logging.info('Response:'+str(response.json()))
    except requests.exceptions.RequestException as e:
        logging.info(f'Request failed: {e}')

def sync_conn(method, endpoint, payload=None):

    url = f'{base_url}/{endpoint}'
    logging.info("URL:"+url)
    if method == 'POST':
        response = requests.post(url, headers=h, json=payload, auth=a)  
    return response.json()

def check_connector_status(method, endpoint, payload=None):

    url = f'{base_url}/{endpoint}'
    try:
        if method == 'GET':
            response = requests.get(url, headers=h, auth=a)
        else:
            raise ValueError('Invalid request method.')
        response.raise_for_status()  # Raise exception for 4xx or 5xx responses

        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Request failed: {e}')
        return None

def check_connector_current_status(connector_id,t1,t2):
    
    scheduled_flag = False
    task_id = ''
    time.sleep(5)
    while True:
        response = check_connector_status('GET', 'connectors/' + connector_id , None)
        sync_state = response['data']['status']['sync_state']
        logging.info('Sync state:'+sync_state)
        if sync_state == 'syncing':
            scheduled_flag
            # Keep checking after 2 seconds
            time.sleep(2)
        elif sync_state == 'scheduled':
            scheduled_flag = True
            break

    if scheduled_flag:
        logging.info("Syncing is scheduled.")
        task_id = t1
        logging.info("Next task:"+task_id)
    else:
        logging.info("Syncing is not scheduled.")
        task_id = t2
        logging.info("Next task:"+task_id)
    
    return task_id
    
with DAG(dag_id="fivetran_glue_dag"
         ,default_args=default_args
         ,schedule_interval=timedelta(days=1)
         ,catchup=False) as dag:
    
    activate_source_connector = PythonOperator(
        task_id = "activate_source_connector"
        ,python_callable = atlas
        ,op_args = activate_src_conn_params
        ,retries=0
    )
    pause_source_connector = PythonOperator(
        task_id = "pause_source_connector"
        ,python_callable = atlas
        ,op_args = pause_src_conn_params
        ,retries=0
    )
    sync_source_connector = PythonOperator(
        task_id = "sync_connector"
        ,python_callable = sync_conn
        ,op_args = sync_src_conn_params
        ,retries=0
    )
    check_current_connector_status = BranchPythonOperator(
        task_id = "check_current_connector_status"
        ,python_callable=check_connector_current_status
        ,op_args = src_status_check_conn_params
    )
    execute_stg_glue_job = GlueJobOperator(
        task_id='execute_stg_glue_job'
        ,job_name='fivetran-airflow-poc-job-staging-tables'
    )
    wait_for_stg_glue_job = GlueJobSensor(
        task_id="wait_for_stg_glue_job"
        ,job_name='fivetran-airflow-poc-job-staging-tables'
        ,run_id=execute_stg_glue_job.output # Job ID extracted from previous Glue Job Operator task
        ,verbose=True  # prints glue job logs in airflow logs
    )
    execute_dim_glue_job = GlueJobOperator(
        task_id='execute_dim_glue_job'
        ,job_name='fivetran-airflow-poc-job-dimension-tables'
    )
    wait_for_dim_glue_job = GlueJobSensor(
        task_id="wait_for_dim_glue_job"
        ,job_name='fivetran-airflow-poc-job-dimension-tables'
        ,run_id=execute_dim_glue_job.output # Job ID extracted from previous Glue Job Operator task
        ,verbose=True  # prints glue job logs in airflow logs
    )
    execute_fact_glue_job = GlueJobOperator(
        task_id='execute_fact_glue_job'
        ,job_name='fivetran-airflow-poc-job-fact-tables'
    )
    wait_for_fact_glue_job = GlueJobSensor(
        task_id="wait_for_fact_glue_job"
        ,job_name='fivetran-airflow-poc-job-fact-tables'
        ,run_id=execute_fact_glue_job.output # Job ID extracted from previous Glue Job Operator task
        ,verbose=True  # prints glue job logs in airflow logs
    )
    none = DummyOperator(
        task_id='none'
    )
    run_dim_dbt_model = BashOperator(
        task_id='run_dim_dbt_model',
        bash_command='dbt run -s models/dimensions/DIM_DATE.sql',
        dag=dag
    )
    run_fact_dbt_model = BashOperator(
        task_id='run_fact_dbt_model',
        bash_command='dbt run -s models/fact/SALES_FACT.sql tag:incremental_model',
        dag=dag
    )
    

activate_source_connector  >> sync_source_connector >> check_current_connector_status
check_current_connector_status >> none

check_current_connector_status >> pause_source_connector >> execute_stg_glue_job
execute_stg_glue_job >> wait_for_stg_glue_job >> execute_dim_glue_job
execute_dim_glue_job >> wait_for_dim_glue_job >> execute_fact_glue_job
execute_fact_glue_job >> wait_for_fact_glue_job >> run_dim_dbt_model >> run_fact_dbt_model
