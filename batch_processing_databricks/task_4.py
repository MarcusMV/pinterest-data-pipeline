## Task 4: Find most popular category in each country

from pyspark.sql.window import Window

joined_df = df_pin.join(df_geo, 'ind', 'inner')

# Group by country and category, and count occurrences of each 
grouped_df = joined_df.groupBy('country', 'category') \
            .agg(F.count('category').alias('category_count'))
    
# Define window specification
window_spec = Window.partitionBy('country').orderBy(F.desc('category_count'))
# Add a row number for each category within each country, order by count
result = grouped_df.withColumn('row_number', F.row_number().over(window_spec))
# Only keep top-ranked category for each country
result = result.filter(F.col('row_number') == 1).drop('row_number')

display(result)