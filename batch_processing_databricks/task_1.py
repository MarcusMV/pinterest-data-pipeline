## Task 1: Cleaning info about pinterest posts to df_pin

from pyspark.sql import functions as F

df = read_from_s3('pin')

# Convert units to numbers for follower_count
repl_dict = {'[kK]': 'e3', '[mM]': 'e6', '[bB]': 'e9'}
for key, val in repl_dict.items():
    df = df.withColumn('follower_count', F.regexp_replace(F.col('follower_count'), key, val))

df = df.withColumn('follower_count', F.expr('double(follower_count)'))
df = df.withColumn('ind', F.coalesce(F.col('ind'), F.col('index')))
df = df.drop('index')

# Inlcude only file path for save_location
df = df.withColumn('save_location', F.regexp_extract(F.col('save_location'), "(\/.*)", 1))
df = df.distinct()
df_pin = df.select('ind', 'unique_id', 'title', 'description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location', 'category')
