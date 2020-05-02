import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Lng

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEY']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # define custom song schema
    songSchema = R([
    Fld("song_id",Str()),
    Fld("year",Int()),
    Fld("duration",Dbl()),
    Fld("title",Str()),
    Fld("artist_id",Str()),
    Fld("artist_name",Str()),
    Fld("artist_location",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_longitude",Dbl())
    ])

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data, schema=songSchema)
    df.printSchema()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table = artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):  
    # define custom log schema
    logSchema = R([
    Fld("artist",Str()),
    Fld("firstName",Str()),
    Fld("gender",Str()),
    Fld("lastName",Str()),
    Fld("level",Str()),
    Fld("page",Str()),
    Fld("sessionId",Int()),
    Fld("song",Str()),
    Fld("ts",Lng()),
    Fld("userAgent",Str()),
    Fld("userId",Str())
    ])
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data, schema=logSchema)
    df.printSchema()
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table = users_table.write.parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/ 1000.0))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/ 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("datetime", get_datetime(df.ts))
    #df.select("timestamp").show(5)
    #df.select("datetime").show(5)
    
    # extract columns to create time table
    time_table = df.select(col("datetime").alias("start_time"),
                                   hour(col("datetime")).alias("hour"),
                                   dayofmonth(col("datetime")).alias("day"), 
                                   weekofyear(col("datetime")).alias("week"), 
                                   month(col("datetime")).alias("month"),
                                   year(col("datetime")).alias("year"))
    
    #time_table.select("hour").show(5)
    #time_table.select("month").show(5)
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    ##song_df = spark.read.parquet("file:///home/workspace/local/songs/year=*/artist_id=*/*")
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df, song_df.title == df.song).select(
                        monotonically_increasing_id().alias('songplay_id'),
                        col("datetime").alias("start_time"),
                        col("userId").alias("user_id"), 
                        "level", 
                        "song_id", 
                        "artist_id", 
                        col("sessionId").alias("session_id"), 
                        col("artist_location").alias("location"), 
                        col("userAgent").alias("user_agent"),
                        month(col("datetime")).alias("month"),
                        year(col("datetime")).alias("year"))
    
    #songplays_table.show(5)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    # process the data from local environment
    #input_data = 'data/'
    #output_data = 'local/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
