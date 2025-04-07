from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# Extract users who frequently listen to music between 12 AM and 5 AM based on their listening timestamps.
def late_listeners(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Get User and count of total streams
    total_counts = streams_df.groupBy("UserID").agg(
        F.count("*").alias("total_streams")
    )

    # Get User and count total late streams
    late_counts = streams_df.groupBy("UserID").agg(
        F.sum(
            F.when(
                F.hour("Timestamp").between(0, 5),
                1
            )
            .otherwise(0)
        ).alias("total_late_streams")
    )

    user_genre_percentage = late_counts.join(total_counts, "UserID").withColumn(
        "LatePercentage", F.round(F.col("total_late_streams") / F.col("total_streams"), 2)
    )

    return user_genre_percentage.orderBy(F.desc("LatePercentage")).select("UserID", "LatePercentage")
    

# Save result
late_listeners(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/late_listeners", header=True)
