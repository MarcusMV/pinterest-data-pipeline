## Task 8: Find the median follower count for different age groups

window_spec_rownum = Window.partitionBy('age_group').orderBy(F.desc('follower_count'))

window_spec_count = Window.partitionBy('age_group')

# Add row number and for each post in each age group, and get count
result = (df.withColumn('row_number', F.row_number().over(window_spec_rownum))
          .withColumn('total_rows', F.count('*').over(window_spec_count)))

# Identify rows that contribute to the median for odd and even counts for each age_group
df_median = result.withColumn(
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

# Filter rows and compute median for each age group
df_median_filtered = df_median.filter(F.col('is_median'))

median_result = df_median_filtered.groupBy('age_group').agg(F.avg('follower_count').alias('median_follower_count'))

# display(df_median_filtered)
display(median_result)