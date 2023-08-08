## Task 5: Find which was the most popular category in each year

joined_df = df_pin.join(df_geo, 'ind', 'inner')

# Group by year and category, and count occurerences of each category
grouped_df = joined_df.groupBy(year('timestamp').alias('post_year'), 'category').agg(F.count('category').alias('category_count'))

# Define window specification
window_spec = Window.partitionBy('post_year').orderBy(F.desc('category_count'))

# Add row number for each category within each year, order by count
result = grouped_df.withColumn('row_number', F.row_number().over(window_spec))

# Only keep top-ranked category for each year between 2018 to 2022
result = result.filter(F.col('row_number') == 1).drop('row_number').filter((F.col('post_year') >= '2018') & (F.col('post_year') <= '2022'))

display(result)