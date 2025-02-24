from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Music Streams KPI Processing") \
    .getOrCreate()

# Define S3 bucket and file paths
bucket_name = 'music-streams-data-engineering-batch-project'
songs_file_path = f's3a://{bucket_name}/music_streams/songs/'
users_file_path = f's3a://{bucket_name}/music_streams/users/'
user_streams_path = f's3a://{bucket_name}/music_streams/user-streams/'


# Read the CSV files into DataFrames
songs_data = spark.read.csv(songs_file_path, header=True, inferSchema=True)
users_data = spark.read.csv(users_file_path, header=True, inferSchema=True)
streams_data = spark.read.csv(user_streams_path + "*", header=True, inferSchema=True)

streams_data = streams_data.withColumn("listen_date", col("listen_time").cast("date"))
streams_data = streams_data.withColumn("listen_hour", expr("HOUR(listen_time)"))

# Merge datasets
full_data = streams_data.join(songs_data, on="track_id").join(users_data, on="user_id")

# KPI 1: Hourly Unique Listeners
hourly_unique_listeners = full_data.groupBy("listen_date", "listen_hour") \
    .agg(countDistinct("user_id").alias("unique_listeners"))

# KPI 2: Top Listened Artist of the Hour
artist_listen_counts = full_data.groupBy("listen_date", "listen_hour", "artists") \
    .agg(count("*").alias("listen_counts"))

window_spec = Window.partitionBy("listen_date", "listen_hour").orderBy(col("listen_counts").desc())

top_artist = artist_listen_counts.withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .drop("rank") \
    .withColumnRenamed("artists", "top_artist")

# KPI 3: Listening Sessions per User per Hour
full_data = full_data.withColumn("session_id", expr("concat(user_id, '-', listen_time)"))

sessions_per_user = full_data.groupBy("listen_date", "listen_hour", "user_id") \
    .agg(countDistinct("session_id").alias("session_count"))

avg_sessions_per_user = sessions_per_user.groupBy("listen_date", "listen_hour") \
    .agg(mean("session_count").alias("avg_sessions_per_user"))

# KPI 4: Hourly Track Diversity Index
track_diversity = full_data.groupBy("listen_date", "listen_hour") \
    .agg(countDistinct("track_id").alias("unique_tracks"), count("*").alias("total_streams"))

track_diversity = track_diversity.withColumn("diversity_index", col("unique_tracks") / col("total_streams"))

# KPI 5: Most Engaged User Group by Age per Hour
age_bins = [0, 25, 35, 45, 55, 65, 100]
age_labels = ["18-25", "26-35", "36-45", "46-55", "56-65", "66+"]
users_data = users_data.withColumn("age_group", expr(f"CASE \
    WHEN user_age <= 25 THEN '18-25' \
    WHEN user_age <= 35 THEN '26-35' \
    WHEN user_age <= 45 THEN '36-45' \
    WHEN user_age <= 55 THEN '46-55' \
    WHEN user_age <= 65 THEN '56-65' \
    ELSE '66+' END"))

user_group_engagement = full_data.join(users_data, on="user_id") \
    .groupBy("listen_date", "listen_hour", "age_group") \
    .agg(count("*").alias("streams"))

window_spec_age = Window.partitionBy("listen_date", "listen_hour").orderBy(col("streams").desc())

most_engaged_group = user_group_engagement.withColumn("rank", row_number().over(window_spec_age)) \
    .filter(col("rank") == 1) \
    .drop("rank") \
    .withColumnRenamed("age_group", "most_engaged_age_group")

# Combine all KPIs
final_kpis = hourly_unique_listeners.join(top_artist, on=["listen_date", "listen_hour"], how="left") \
    .join(avg_sessions_per_user, on=["listen_date", "listen_hour"], how="left") \
    .join(track_diversity.select("listen_date", "listen_hour", "diversity_index"), on=["listen_date", "listen_hour"], how="left") \
    .join(most_engaged_group.select("listen_date", "listen_hour", "most_engaged_age_group"), on=["listen_date", "listen_hour"], how="left")

# Select the final columns explicitly to match the Redshift table schema
final_kpis = final_kpis.select("listen_date", "listen_hour", "unique_listeners", "listen_counts", "top_artist",
                               "avg_sessions_per_user", "diversity_index", "most_engaged_age_group")

# Show final results
final_kpis.show()

output_path = f's3a://{bucket_name}/music_streams/output/hourly-kpis/'
final_kpis.write.mode("overwrite").csv(output_path, header=True)
spark.stop()