# Clean df_geo table

# Define schema for JSON data
fields = [
    ('ind', IntegerType()), 
    ('timestamp', TimestampType()), 
    ('latitude', FloatType()), 
    ('longitude', FloatType()), 
    ('country', StringType()), 
]

schema = StructType([StructField(field[0], field[1]) for field in fields])

# Parse JSON in the 'data' column
df = df_geo.withColumn('parsed_data', from_json(col('data'), schema))

# Extract individual fields from parsed_data column
for field in fields:
    df = df.withColumn(field[0], df.parsed_data.getField(field[0]))

# Store lat and long to array in coordinates column
df = df.withColumn('coordinates', F.array('latitude', 'longitude'))
df = df.drop('latitude', 'longitude')

df = df.distinct()
df_geo_clean = df.select('ind', 'country', 'coordinates', 'timestamp')

query_geo = write_to_delta_table(df_geo_clean, '12c5e9eb47cb_geo_table')
# display(df_geo_clean)