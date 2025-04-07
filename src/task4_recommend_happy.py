from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load DataFrames
songs_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/songs.csv")
streams_df = spark.read.option("header", True).csv("/opt/bitnami/spark/Music/input/streams.csv")

# Find users who primarily listen to "Sad" songs and recommend up to 3 "Happy" songs they haven’t played yet.
def reccomend_happy(songs_df: DataFrame, streams_df: DataFrame) -> DataFrame:
    # Join songs to streams
    df = (
        streams_df
        .join(songs_df, "SongID", "inner")
    )

    # Find users with a Sad/Happy ratio over 1
    sad_listeners = df.groupBy("UserID").agg(
        (
            F.sum(F.when(df.Mood == "Sad", 1).otherwise(0)) /
            F.sum(F.when(df.Mood == "Happy", 1).otherwise(0)) 
        ).alias("Happy_Sad_Ratio")
    ).filter(F.col("Happy_Sad_Ratio") > 1)

    # Get songs which the user has listened to
    songs_streamed_per_user = streams_df.select("UserID", "SongID").distinct()

    # Get happy songs
    happy_songs = songs_df.filter(songs_df.Mood == "Happy").select("SongID", "Title")

    # Recommend happy songs
    recommendations = (
        sad_listeners
        .crossJoin(happy_songs) # Pair happy songs to each user
        .join(songs_streamed_per_user, ["UserID", "SongID"], "left_anti") # Join and remove duplicate songs
    )

    # Select 3 out of the list to recommend
    return (
        recommendations.withColumn("rank", F.row_number().over(
            Window.partitionBy("UserID").orderBy(F.rand())
        ))
        .filter(F.col("rank") <= 3)
        .drop("rank")
    )


# Save result
reccomend_happy(songs_df, streams_df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/Music/output/recommend_happy", header=True)
