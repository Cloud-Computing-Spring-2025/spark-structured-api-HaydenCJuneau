from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import avg, round

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# Compute the average duration (in seconds) for each song based on user play history.
def average_listen_time(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Join songs to streams
    df = (
        streams_df
        .join(songs_df, "SongID", "inner")
        .select("SongID", "Title", "DurationSec")
    )

    return df.groupBy("SongID", "Title").agg(
        round(avg(df.DurationSec), 2).alias("avg_listen_time_sec")
    )
    

# Save result
average_listen_time(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/average_listen_time", header=True)
