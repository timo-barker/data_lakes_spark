#!/usr/bin/env python3
# coding: utf-8

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id
import getpass


config = configparser.ConfigParser()
config.read('dl.cfg')

if config['AWS']['AWS_ACCESS_KEY_ID'] is None:
    os.environ['AWS_ACCESS_KEY_ID'] = getpass.getpass(prompt='AWS_ACCESS_KEY_ID:')
else:
    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
if config['AWS']['AWS_SECRET_ACCESS_KEY'] is None:
    os.environ['AWS_SECRET_ACCESS_KEY'] = getpass.getpass(prompt='AWS_SECRET_ACCESS_KEY:')
else:
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Create a spark session
    '''
    
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Extract song and artist information from song data and save them to S3.
    
    Keyword arguements:
    spark -- the Spark session object to perform procs on 
    input_data -- the S3 bucket source to read files from
    output_data -- the S3 bucket target to write files to
    '''
    
    # get filepath to song data file
    song_data = 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(input_data + song_data)
    
    # create temp view for SQL wrangling
    df.createOrReplaceTempView('staging_songs')

    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT
            song_id
           ,title
           ,artist_id
           ,year
           ,duration
        FROM
          (
           SELECT
               song_id
              ,title
              ,artist_id
              ,year
              ,duration
              ,ROW_NUMBER() OVER(PARTITION BY song_id
                                 ORDER BY year DESC
                                ) AS row_num
           FROM staging_songs
           WHERE song_id IS NOT NULL
          )
        AS cte_staging_songs
        WHERE row_num = 1
    ''')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT
            artist_id
           ,artist_name AS name
           ,artist_location AS location
           ,artist_latitude AS latitude
           ,artist_longitude AS longitude
        FROM
          (
           SELECT
               artist_id
              ,artist_name
              ,artist_location
              ,artist_latitude
              ,artist_longitude
              ,ROW_NUMBER() OVER(PARTITION BY artist_id
                                 ORDER BY year DESC
                                ) AS row_num
           FROM staging_songs
           WHERE artist_id IS NOT NULL
          )
        AS cte_staging_songs
        WHERE row_num = 1
    ''')
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    '''
    Extract user, time, and songplay information from log data and save them to S3.
    
    Keyword arguements:
    spark -- the Spark session object to perform procs on 
    input_data -- the S3 bucket source to read files from
    output_data -- the S3 bucket target to write files to
    '''
    
    # get filepath to log data file
    log_data = 'log-data/*/*/*.json'

    # read log data file
    df = spark.read.json(input_data + log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create temp view for SQL wrangling
    df.createOrReplaceTempView('staging_events')

    # extract columns for users table    
    users_table = spark.sql('''
        SELECT
            user_Id AS user_id
           ,firstName AS first_name
           ,lastName AS last_name
           ,gender
           ,level
        FROM
          (
           SELECT
               user_Id
              ,firstName
              ,lastName
              ,gender
              ,level
              ,ROW_NUMBER() OVER(PARTITION BY user_Id
                                 ORDER BY ts DESC
                                ) AS row_num
           FROM staging_events
           WHERE user_Id IS NOT NULL
          )
        AS staging_events
        WHERE row_num = 1
    ''')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
        .dropna() \
        .dropDuplicates() \
        .withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet')

    # read in time data to use for songplays table
    time_table.createOrReplaceTempView('staging_time')

    # create songplay id column
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    
    # replace temp view with added start_time and songplay_id columns
    df.createOrReplaceTempView('staging_events')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
       SELECT
           se.songplay_id
           st.start_time
          ,se.user_Id AS user_id
          ,se.level
          ,ss.song_id
          ,ss.artist_id
          ,se.sessionId AS session_id
          ,se.location
          ,se.userAgent AS user_agent
          ,st.year
          ,st.month
       FROM staging_songs AS ss
           INNER JOIN staging_events AS se
               ON ss.artist_name = se.artist
               AND ss.title = se.song
           INNER JOIN staging_time AS st
               ON se.start_time = st.start_time
    ''')  
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://timo-udacity-proj-4/'
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

