from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# For each user, calculate the proportion of their plays that belong to their most-listened genre. Output users with a loyalty score above 0.8.
def genre_loyalty(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Join songs to streams
    df = streams_df.join(songs_df, streams_df.SongID == songs_df.SongID, "inner")
    
    # Get User and count of total streams
    total_counts = df.groupBy("UserID").agg(
        F.count("*").alias("total_streams")
    )

    # Get User and Mood and count total streams
    genre_counts = df.groupBy("UserID", "Genre").agg(
        F.count("*").alias("genre_total_streams")
    )

    user_genre_percentage = genre_counts.join(total_counts, "UserID").withColumn(
        "GenrePercentage", F.round(F.col("genre_total_streams") / F.col("total_streams"), 2)
    )

    return user_genre_percentage.filter(F.col("GenrePercentage") > 0.5).select("UserID", "Genre", "GenrePercentage")
    

# Save result
genre_loyalty(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/genre_loyalty", header=True)
