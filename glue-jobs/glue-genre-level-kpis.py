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
songs_df = spark.read.csv(songs_file_path, header=True, inferSchema=True)
users_df = spark.read.csv(users_file_path, header=True, inferSchema=True)
user_streams_df = spark.read.csv(user_streams_path + "*", header=True, inferSchema=True)

user_streams_df = user_streams_df.withColumn("listen_date", to_date(col("listen_time")))

merged_data = user_streams_df.join(songs_df, on ='track_id', how='left')

# Convert duration from milliseconds to seconds
merged_data = merged_data.withColumn("duration_seconds", col("duration_ms") / 1000)

# Calculate genre listen count
genre_listen_count = merged_data.groupBy("listen_date", "track_genre").agg(count("*").alias("listen_count"))

# Calculate average duration
avg_duration = merged_data.groupBy("listen_date", "track_genre").agg(mean("duration_seconds").alias("average_duration"))

# Calculate total listens per day
total_listens = merged_data.groupBy("listen_date").agg(count("*").alias("total_listens"))

# Join total listens to genre listen count
genre_listen_count = genre_listen_count.join(total_listens, on="listen_date", how="inner")

# Calculate popularity index
genre_listen_count = genre_listen_count.withColumn("popularity_index", col("listen_count") / col("total_listens"))

# Identify most popular track per genre per day
most_popular_track = merged_data.groupBy("listen_date", "track_genre", "track_id").agg(count("*").alias("track_count"))

# Sort and select most popular track per genre per day
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("listen_date", "track_genre").orderBy(col("track_count").desc())

most_popular_track = most_popular_track.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
most_popular_track = most_popular_track.drop("rank").withColumnRenamed("track_id", "most_popular_track_id")

# Select final KPIs
final_kpis = genre_listen_count.select("listen_date", "track_genre", "listen_count", "popularity_index")

# Merge with average duration and most popular track
final_kpis = final_kpis.join(avg_duration, on=["listen_date", "track_genre"], how="inner")
final_kpis = final_kpis.join(most_popular_track.select("listen_date", "track_genre", "most_popular_track_id"), on=["listen_date", "track_genre"], how="inner")

# Show final results
final_kpis.show()

output_path = f's3a://{bucket_name}/music_streams/output/genre_level_kpis/'
final_kpis.write.mode("overwrite").csv(output_path, header=True)
spark.stop()