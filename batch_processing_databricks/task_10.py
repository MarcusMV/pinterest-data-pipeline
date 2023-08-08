## Task 10: Find the median follower count of users based on joining year

# one-to-one user to follower_count
df_user = joined_df.dropDuplicates(['user_name'])

filtered_df = df_user.withColumn('year_joined', F.year(F.col('date_joined')))

window_spec_rownum = Window.partitionBy('year_joined').orderBy(F.desc('follower_count'))
window_spec_count = Window.partitionBy('year_joined')

# Apply row_number and count for users per year
df = (filtered_df
      .withColumn('row_number', F.row_number().over(window_spec_rownum))
      .withColumn('total_rows', F.count('*').over(window_spec_count)))

# Identify rows that contribute to the median for odd and even number of rows for each year
df_median = df.withColumn(
    "is_median",
    F.when(
        (F.col('total_rows') % 2 == 1) &
        (F.col('row_number') == (F.col('total_rows') + 1) / 2), True
    ).otherwise(
        F.when(
            (F.col('total_rows') % 2 == 0) &
            ((F.col('row_number') == F.col('total_rows') / 2) |
             (F.col('row_number') == F.col('total_rows') / 2 + 1)), True
        ).otherwise(False)
    )
)

# Compute median for each year
median_result = df_median.filter(F.col('is_median')).groupBy('year_joined').agg(F.avg('follower_count').alias('median_follower_count'))

display(median_result)