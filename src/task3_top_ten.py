from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# Determine which songs were played the most in the current week and return the top 10 based on play count.
def weekly_top_ten(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Streams which occurred this week
    week_streams = streams_df.filter(
        streams_df.Timestamp >= F.date_sub(F.current_date(), 7)
    )

    # Join songs to streams
    df = (
        week_streams
        .join(songs_df, "SongID", "inner")
    )

    summed = (
        df.groupBy("SongID", "Title")
        .count()
        .withColumnRenamed("count", "total_streams")
    )

    return summed.orderBy("total_streams", ascending=False).limit(10)
    

# Save result
weekly_top_ten(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/weekly_top_ten", header=True)
