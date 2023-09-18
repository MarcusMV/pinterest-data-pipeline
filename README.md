# Pinterest Data Pipeline

## Kafka on EC2

- Configure Kafka client to use AWS IAM authentication to connect to MSK cluster
- Create Kafka topics:
    - .pin for Pinterest posts data
    - .geo for geolocation data and
    - .user for post user data
- Create plugin-connector pair with MSK Connect to pass data through cluster and automatically save in dedicated S3 bucket
- Configure an API in API Gateway to send data to cluster, and then store in S3 using connector created

## Populate topics on S3

- Access EC2 instance
- Start REST proxy in confluent-7.2.0/bin
- Run user_posting_emulation.py

This script will return entries from a database containing infrastructure similar to that from Pinterest.

## Read from S3 to Databricks

- Each file in ./batch_processing_databricks represents a cell on a databricks notebook
- Mount S3 bucket to Databricks with mount_s3_to_databricks.py
- Read objects from each topic to dataframes with read_from_s3.py
- Perform data cleaning and computations using Spark on Databricks

## Getting started with Airflow on Windows:
https://medium.com/international-school-of-ai-data-science/setting-up-apache-airflow-in-windows-using-wsl-dbb910c04ae0

Run airflow scheduler in WSL2 instance #1 - activate virtual environment, cd to airflow, run scheduler detached:

`source airflow_env/bin/activate`
`cd $AIRFLOW_HOME`
`airflow scheduler -d`

And in terminal #2 - run webserver detached for UI:

`source airflow_env/bin/activate`
`cd $AIRFLOW_HOME`
`airflow webserver -d`

airflow users list 
Sign into DAG UI at localhost:8080/

Add DAGs in /airflow/dags to the UI on localhost:
`airflow db init`

Run DAGs in airflow/dags:
`airflow dags unpause dag_name`

In WSL2, Windows paths are mapped differently. Like so: with /mnt/
`/mnt/c/Users/Marcu/OneDrive/Documents/AiCore/Weather_Airflow`

Assign airflow variables under 'Admin > Variables'. Import and access:

`from airflow.models import Variable`
`weather_dir = Variable.get("weather_dir")`

## Configure GitHub access on WSL2 Instance:

Get system user:
  `ps aux | grep airflow`

Set git config globally for system user running airflow:
  `sudo -u marcus git config --global user.name "Marcus Vo"`
  `sudo -u marcus git config --global user.email "MarcusMV@hotmail.co.uk"`

Genrate PAT from GitHub or use SSH.

Authenticate access to GitHub by specifying PAT:
  `git remote set-url origin https://<PAT_HERE>:x-oauth-basic@github.com/MarcusMV/Weather_Airflow.git`

Create `git_operations.py` file to contain git actions to add, commit, and push to remote repository on GitHub

## Batch Processing: AWS MMA

1. Create an API token in Databricks to connect to AWS account
2. Set up the MWAA-Databricks connection
3. Create the `requirements.txt` file

`/12c5e9eb47cb_dag.py` contains Airflow DAG that will trigger a Databricks notebook to be run on a 5-minute schedule. DAG is uploaded to `dags` folder on `mwaa-dags-bucket` on Amazon S3.

Contents of folder `/batch_processing_databricks` are local copy of cells of databricks notebook that are triggered on `/12c5e9eb47cb_dag.py` runs. Path specified points at databricks notebook.

*DAG might fail if  notebook contains the commands for mounting the S3 bucket, as this is something that should only be done once. Comment out `dbutils.fs.mount` if necessary.