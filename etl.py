import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

READ_S3_BUCKET = config['S3']['READ_S3_BUCKET']
WRITE_S3_BUCKET = config['S3']['WRITE_S3_BUCKET']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data and create songs and artists table.
    
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Destination S3 endpoint
    """
    # get filepath to song data file
    song_data = os.path.join(input_data + "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data_view")
    songs_table = spark.sql("SELECT song_id, title, artist_id, year, duration FROM song_data_view")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(path=output_data + "song_table.parquet")

    # extract columns to create artists table
    artist_table = spark.sql("SELECT artist_id, artist_name as name, artist_location as location, artist_latitude as latitude, artist_longitude as longitude FROM song_data_view")   
    
    # write artists table to parquet files
    artist_table.write.mode("overwrite").parquet(path=output_data + "artist_table.parquet")


def process_log_data(spark, input_data, output_data):
    """Process log data and create users, time and songplays table.
    Args:
        spark {object}: SparkSession object
        input_data {object}: Source S3 endpoint
        output_data {object}: Target S3 endpoint
    """
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("log_data_view")
    df = spark.sql("SELECT * FROM log_data_view WHERE page='NextSong'") 

    # extract columns for users table    
    artists_table = spark.sql("SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level FROM log_data_view")
    
    # write users table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + "user_table.parquet")

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", (df["ts"]/1000).cast("timestamp"))  
    
    # extract columns to create time table
    df = df.withColumn("hour", hour(df["start_time"]))
    df = df.withColumn("day", dayofmonth(df["start_time"]))
    df = df.withColumn("week", weekofyear(df["start_time"]))
    df = df.withColumn("month", month(df["start_time"]))
    df = df.withColumn("year", year(df["start_time"]))
    df = df.withColumn("weekday", date_format(df["start_time"], "E"))
    
    time_table = df.select("start_time", "weekday", "year", "month", "week", "day", "hour").distinct() 
     
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(path=output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_data_view")  

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title)\
                              & (df.artist == song_df.artist_name)\
                              & (df.length == song_df.duration), "inner")\
    .distinct()\
    .select("start_time", "userId", "level", "song_id",\
            "artist_id", "sessionId","location","userAgent", df["year"], df["month"])\
    .withColumn("songplay_id", monotonically_increasing_id()) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(path=output_data + "songplays_table.parquet")


def main():
    spark = create_spark_session()
    
    process_song_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)    
    process_log_data(spark, READ_S3_BUCKET, WRITE_S3_BUCKET)

if __name__ == "__main__":
    main()
