import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, desc
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

def create_spark_session():
    """
      Creates Spark session and returns spark instance
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
      Function to process song data
      Parameters: 
            spark: This is the newly created Spark instance
            input_data  : This is the filepath for the song data input
            output_data : This is the filepath for the song data output          
    """    
    # get filepath to song data fil*
    #song_data = os.path.join(input_data,'song_data/A/A/A/TRAAAAW128F429D538.json')
    song_data = os.path.join(input_data,'song_data/A/A/A/*')
    print(song_data)
        
    # read song data file
    df = spark.read.format("json").load(song_data,inferSchema=True, header=True)
    #df.limit(5).printSchema()
    #print(df.count())

    # extract columns to create songs table
    songs_table = df['num_songs', 'artist_id', 'artist_latitude', 'artist_longitude', 'artist_location','artist_name', 'song_id', 'title', 'duration',       'year']
    
    print(songs_table)
    df.createOrReplaceTempView("SongData")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet('songs.pq')
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data,"song"))

    # extract columns to create artists table
    artists_table = df["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    
    print(artists_table)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").partitionBy("artist_id").parquet('artists.pq')
    artists_table.write.mode("overwrite").partitionBy("artist_id").parquet(os.path.join(output_data,"artist"))


def process_log_data(spark, input_data, output_data):
    """
      Function to process Log data
      Parameters: 
            spark: This is the newly created Spark instance
            input_data  : This is the filepath for the log data input
            output_data : This is the filepath for the log data output          
    """        
    # get filepath to log data file
    log_data = os.path.join(input_data,"log-data/*.json")
    #log_data = os.path.join(input_data,"log_data/*")
    
    # read log data file
    df_log = spark.read.format("json").load(log_data,inferSchema=True, header=True)
    df_log.printSchema()
    print(df_log.count())
    
    # filter by actions for song plays
    df1 = df_log[df_log.page == "NextSong"]

    # extract columns for users table    
    users_table = df1['userId', 'firstName', 'lastName', 'gender', 'level']
    
    users_table.printSchema()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet('user.pq')
    users_table.write.mode("overwrite").parquet(os.path.join(output_data,"user"))
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), T.TimestampType())
                        
    df1 = df1.withColumn("timestamp",get_timestamp(df1['ts']))
    df1.show(5)
    df1.printSchema()
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), T.DateType())
    df1 = df1.withColumn("datetime",get_datetime(df1['ts']))
    df1.show(5)
    df1.printSchema()
    df1.dropna()
   
    df1 = df1.withColumn("day",F.dayofmonth(df1['datetime']))
    df1 = df1.withColumn("month",F.month(df1['datetime']))
    df1 = df1.withColumn("year",F.year(df1['datetime']))
    df1 = df1.withColumn("week",F.weekofyear(df1['datetime']))
    df1 = df1.withColumn("hour",F.hour(df1['datetime']))
    df1.show()

    df1.printSchema()
    #df1.limit(5).toPandas()
    
    # extract columns to create time table
    print("Extracting time table columns")
    time_table = df1['ts', 'timestamp', 'datetime', 'month', 'day', 'year', 'week', 'hour']
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").parquet('time.pq')    
    time_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data,"time"))

    # read in song data to use for songplays table   
    print("Creating Temp View")
    df1.createOrReplaceTempView("LogData")

    songplays_df = spark.sql("""
    SELECT song, length, artist, timestamp, b.year, b.month, userId, level, sessionId, location, userAgent
    FROM songData a
    JOIN LogData b ON (a.artist_id = b.artist
    AND a.song_id = b.song and a.duration = b.length)
    """)
    
    songplays_df.limit(5).printSchema()
    
    #Remove duplicates in songplays table
    print("Dropping Duplicates from songplays")
    songplays_df.dropDuplicates()    
    
    #Remove duplicate entries for same users with different levels. Retain only the latest record.
    print("Dropping Duplicates user records")
    songplays_df.orderBy("userId", F.col("timestamp").desc()).dropDuplicates(["userId"])

    # extract columns from joined song and log datasets to create songplays table 
    print("Extracting songplays columns")
    songplays_table = songplays_df['timestamp', 'year', 'month', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet('songplays.pq')    
    songplays_table.write.mode("overwrite").partitionBy("year","month").parquet(os.path.join(output_data,"songplays"))


def main():
    """
     Function main() - Program execution begins here
    """    
    spark = create_spark_session()
    #spark.SparkContext.setLogLevel("ERROR")
    
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3a://udacity-datalake-proj/"

    input_data = "/home/workspace/data/"
    output_data = "s3a://udacity-datalake-proj/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
