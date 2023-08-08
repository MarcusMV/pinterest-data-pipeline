# Task 11: Find the median follower count of users based on their joining year and age group

# Create age_group and year_joined columns
df = joined_df.withColumn('year_joined', F.year(F.col('date_joined')))
df = df.withColumn('age_group',
                   F.when(F.col('age').between(18, 24), '18-24')
                   .when(F.col('age').between(25, 35), '25-35')
                   .when(F.col('age').between(36, 50), '36-50')
                   .otherwise('+50'))

# Define window specs to partition by age_group and year_joined
window_spec = Window.partitionBy('age_group', 'year_joined').orderBy('follower_count')
window_spec_count = Window.partitionBy('age_group', 'year_joined')

df_with_rownum = (df.withColumn('row_number', F.row_number().over(window_spec))
                  .withColumn('total_rows', F.count('*').over(window_spec_count)))

# Determine which rows correspond to median for each partition
df_median = df_with_rownum.withColumn(
    "is_median",
    F.when(
        (F.col("total_rows") % 2 == 1) & 
        (F.col("row_number") == (F.col("total_rows") + 1) / 2), True
    ).otherwise(
        F.when(
            (F.col("total_rows") % 2 == 0) & 
            ((F.col("row_number") == F.col("total_rows") / 2) | 
             (F.col("row_number") == (F.col("total_rows") / 2) + 1)), True
        ).otherwise(False)
    )
)

# Calculate median
median_result = df_median.filter(F.col('is_median')).groupBy('age_group', 'year_joined').agg(F.avg('follower_count').alias('median_follower_count'))

display(median_result)