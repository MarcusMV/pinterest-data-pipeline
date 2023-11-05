# Clean df_pin table

# Define schema for JSON data
fields = [
    ('index', IntegerType()), 
    ('unique_id', StringType()), 
    ('title', StringType()), 
    ('description', StringType()), 
    ('poster_name', StringType()), 
    ('follower_count', StringType()), 
    ('tag_list', StringType()), 
    ('is_image_or_video', StringType()), 
    ('image_src', StringType()), 
    ('downloaded', StringType()),
    ('save_location', StringType()), 
    ('category', StringType())
]

schema = StructType([StructField(field[0], field[1]) for field in fields])

# Parse JSON in the 'data' column
df = df_pin.withColumn('parsed_data', from_json(col('data'), schema))

# Extract individual fields from parsed_data column, rename index to ind
for field in fields:
    df = df.withColumn(field[0], df.parsed_data.getField(field[0]))
df = df.withColumnRenamed('index', 'ind')

# Replace empty entries and entries with no relevant data in each column with Nones
df = df.withColumn(
    'title',
    when(col('title').contains('No Title Data Available'), None).otherwise(col('title'))
).withColumn(
    'description',
    when(
        col('description').contains('No description available') | 
        col('description').contains('Untitled'),
        None
    ).otherwise(col('description'))
).withColumn(
    'poster_name',
    when(col('poster_name').contains('User Info Error'), None).otherwise(col('poster_name'))
).withColumn(
    'follower_count',
    when(col('follower_count').contains('User Info Error'), None).otherwise(col('follower_count'))
).withColumn(
    'tag_list',
    when(col('tag_list').contains('N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e'), None).otherwise(col('tag_list'))
).withColumn(
    'image_src',
    when(col('image_src').contains('Image src error.'), None).otherwise(col('image_src'))
)

# Convert units to numbers for follower_count
repl_dict = {'[kK]': 'e3', '[mM]': 'e6', '[bB]': 'e9'}
for key, val in repl_dict.items():
    df = df.withColumn('follower_count', F.regexp_replace(F.col('follower_count'), key, val))

df = df.withColumn('follower_count', F.expr('double(follower_count)'))
# Convert follower_count to IntegerType
df = df.withColumn('follower_count', col('follower_count').cast(IntegerType()))

# Inlcude only file path for save_location
df = df.withColumn('save_location', F.regexp_extract(F.col('save_location'), "(\/.*)", 1))

df = df.distinct()
df_pin_clean = df.select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')

query_pin = write_to_delta_table(df_pin_clean, '12c5e9eb47cb_pin_table')
# display(df_pin_clean)
