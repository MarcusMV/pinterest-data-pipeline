from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/marcusmv@hotmail.co.uk/Batch Processing: Databricks 2023-07-17 20:42:08',
}

# Define params for Run Now Operator
notebook_params = {
    'Variable': 5
}

default_args = {
    'owner': 'Marcus',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('12c5e9eb47cb_dag',
         # should be a datetime format
         start_date=datetime(2023, 9, 13),
         schedule_interval=timedelta(minutes=5),
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
