## Task 3: Cleaning info about users to df_users

df = read_from_s3('user')
# concatenate first and last name to user_name col
df = df.withColumn('user_name', concat_ws(' ', 'first_name', 'last_name'))
df = df.drop('first_name', 'last_name')
df = df.withColumn('date_joined', F.to_timestamp('date_joined', 'yyyy-MM-dd HH:mm:ss'))
df = df.distinct()
df_user = df.select('ind', 'user_name', 'age', 'date_joined')
