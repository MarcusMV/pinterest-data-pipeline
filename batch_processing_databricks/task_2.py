## Task 2: Cleaning info about geolocation of posts to df_geo

df = read_from_s3('geo')

# Store lat and long to array in coordinates column
df = df.withColumn('coordinates', F.array('latitude', 'longitude'))
df = df.drop('latitude', 'longitude')
# Convert to timestamp
df = df.withColumn('timestamp', F.to_timestamp('timestamp', 'yyyy-MM-dd HH:mm:ss'))
df = df.distinct()
df_geo = df.select('ind', 'country', 'coordinates', 'timestamp')
