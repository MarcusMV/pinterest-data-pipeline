## Task 6: Find the user with the most followers in each country

joined_df = df_pin.join(df_user, 'ind', 'inner').join(df_geo, 'ind', 'inner')

# Step 1: For each country, find the user with the most followers
# Define window specification
window_spec = Window.partitionBy('country').orderBy(F.desc('follower_count'))

# Add row number for each post in each country
result = joined_df.withColumn('row_number', F.row_number().over(window_spec))

# Filter to only keep top-ranked user/follower_count
result = result.filter(F.col('row_number') == 1).select('country', 'poster_name', 'follower_count')

# Step 2: Find the country with the user with the most followers
highest = result.orderBy(F.desc('follower_count')).limit(1).drop('poster_name')

display(highest)