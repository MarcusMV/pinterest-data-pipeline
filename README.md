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