"""
Must be run on databricks with S3 bucket mounted
"""

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# AWS S3 bucket name
AWS_S3_BUCKET = "user-12c5e9eb47cb-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount_name"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)


# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension

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
    # display(df)
    return df

topics = {'pin', 'geo', 'user'}
dfs_container = {f'df_{key}': read_from_s3(key) for key in topics}
print(dfs_container.keys())
display(dfs_container['df_geo'])
display(dfs_container['df_pin'])
display(dfs_container['df_user'])