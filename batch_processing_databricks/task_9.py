## Task 9: Find how many users have joined each year?

grouped_df = joined_df.groupBy(year('date_joined').alias('year_joined')).agg(F.countDistinct('user_name').alias('number_users_joined'))

display(grouped_df)