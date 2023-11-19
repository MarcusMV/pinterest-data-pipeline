# read objects from s3 topics to spark df
from pyspark.sql.types import *

def read_from_s3(topic):
    file_location = f"/mnt/mount_name/topics/12c5e9eb47cb.{topic}/partition=0/*.json" 
    file_type = "json"
    # Ask Spark to infer the schema
    infer_schema = "true"
    # Read in JSONs from mounted S3 bucket
    df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .load(file_location)
    # Display Spark dataframe to check its content
    return df

topics = {'pin', 'geo', 'user'}
dfs_container = {f'df_{key}': read_from_s3(key) for key in topics}

display(dfs_container['df_pin'])