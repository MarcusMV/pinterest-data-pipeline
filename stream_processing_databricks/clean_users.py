from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from functools import reduce
from operator import and_

# Clean df_user table
# Define schema for JSON data
fields = [
    ('ind', IntegerType()), 
    ('first_name', StringType()), 
    ('last_name', StringType()), 
    ('age', IntegerType()), 
    ('date_joined', TimestampType()), 
]

schema = StructType([StructField(field[0], field[1]) for field in fields])

# Parse JSON in the 'data' column
df = df_user.withColumn('parsed_data', from_json(col('data'), schema))

# Remove records where any keys are null/did not match schema
conditions = [col(f'parsed_data.{field[0]}').isNotNull() for field in fields]
combined_condition = reduce(and_, conditions)
df = df.filter(combined_condition)

# Extract individual fields from parsed_data column
for field in fields:
    df = df.withColumn(field[0], df.parsed_data.getField(field[0]))

# Concatenate first_name and last_name to user_name column
df = df.withColumn('user_name', F.concat_ws(' ', df.first_name, df.last_name))

df_user_clean = df.select('ind', 'user_name', 'age', 'date_joined')

query_user = write_to_delta_table(df_user_clean, '12c5e9eb47cb_user_table')
# display(df_user_clean)
