## Task 7: Find the most popular category for different age groups

df = joined_df.withColumn('age_group', 
                          when((joined_df.age >= 18) & (joined_df.age <= 24), '18-24')
                          .when((joined_df.age >= 25) & (joined_df.age <= 35), '25-35')
                          .when((joined_df.age >= 36) & (joined_df.age <= 50), '36-50')
                          .when(joined_df.age > 50, '+50'))

# Group by age_group and category, and count occurences of each category
grouped_df = df.groupBy('age_group', 'category').agg(F.count('category').alias('category_count'))

# Define spec to partition by age_group and order by category_count
window_spec = Window.partitionBy('age_group').orderBy(F.desc('category_count'))

# Add row number for each category in each age_group
result = grouped_df.withColumn('row_number', F.row_number().over(window_spec))

# Only keep top-ranked category in each age_group
result = result.filter(F.col('row_number') == 1).drop('row_number')

display(result)