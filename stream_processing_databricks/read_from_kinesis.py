from pyspark.sql.streaming import StreamingQuery

def read_from_kinesis(topic):
    df = spark \
    .readStream \
    .format('kinesis') \
    .option('streamName', f'streaming-12c5e9eb47cb-{topic}') \
    .option('initialPosition','earliest') \
    .option('region','us-east-1') \
    .option('awsAccessKey', ACCESS_KEY) \
    .option('awsSecretKey', SECRET_KEY) \
    .load()

    return df

def write_to_delta_table(df: DataFrame, table_name: str) -> StreamingQuery:
    # Delete checkpoint location first
    dbutils.fs.rm(f"/tmp/kinesis/{table_name}_checkpoints/", True)

    query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/kinesis/{table_name}_checkpoints/") \
    .table(table_name)

    print(f"Writing to delta table {table_name}..")
    return query
