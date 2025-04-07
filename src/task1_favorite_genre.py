from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import desc, count, rank

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# Identify the most listened-to genre for each user by counting how many times they played songs in each genre
def detect_favorite_genre(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Join songs to streams
    df = streams_df.join(songs_df, streams_df.SongID == songs_df.SongID, "inner")

    # Group by user and genre jointly
    user_grouped = df.groupBy("UserID", "Genre").agg(count("*").alias("count"))

    # Use window function to rank users top genres
    user_ranked = user_grouped.withColumn("rank", rank().over(
        Window.partitionBy("UserID").orderBy(desc("count"))
    ))

    # Only add the top ranked genre
    return user_ranked.filter(user_ranked.rank == 1).drop("rank", "count")
    

# Save result
detect_favorite_genre(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/favorite_genre", header=True)
