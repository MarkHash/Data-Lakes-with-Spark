import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['spark-cluster']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['spark-cluster']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates().sort('song_id')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs_table.parquet'))

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates().sort('artist_id')

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists_table.parquet'))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').where(df.userId != '').dropDuplicates().sort('userId')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users_table.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    time_df = df.select('song', 'ts').dropDuplicates().withColumn("start_time", get_timestamp(df.ts))

    # extract columns to create time table
    time_df.createOrReplaceTempView('time_df')
    time_table = spark.sql('''
        SELECT start_time,
        hour(start_time)    AS hour,
        day(start_time)     AS day,
        weekofyear(start_time)    AS week,
        month(start_time)   AS month,
        year(start_time)    AS year,
        dayofweek(start_time) AS weekday
        FROM time_df
        ''')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time_table.parquet'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs_table.parquet'))

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('song_df')
    songplays_table = spark.sql('''
        SELECT  t.start_time,
                t.userId,
                t.level,
                s.song_id,
                s.artist_id,
                t.sessionId,
                t.location,
                t.useragent
        FROM time_df t
        JOIN song_df s ON (t.song = s.title)
        ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('start_time').parquet(os.path.join(output_data, 'songplays_table.parquet'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udacity-nanodegree-dataengineer-markhash/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
