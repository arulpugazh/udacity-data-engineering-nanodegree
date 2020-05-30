import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'duration', 'year', 'artist_id']).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(
        ['artist_id', 'artist_name', 'artist_latitude', 'artist_longitude', 'artist_location']).distinct()

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).distinct()

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts))
    df = df.withColumn('timestamp', get_timestamp("ts"))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('datetime', get_datetime("ts"))

    # extract columns to create time table
    df.createOrReplaceTempView('time_df')

    time_table = spark.sql('''
    SELECT DISTINCT datetime as start_time, 
    hour(timestamp) as hour,
    dayofmonth(timestamp) as day,
    dayofweek(timestamp) as weekday,
    weekofyear(timestamp) as week,
    month(timestamp) as month,
    year(timestamp) as year
    FROM time_df''')

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'timetable.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs.parquet')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist == song_df.artist_id) & (df.song == song_df.song_id))
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table = songplays_df.join(
        time_table,
        songplays_table.ts == time_table.start_time, 'left'
    ).drop(songplays_table.year)

    songplays_table.createOrReplaceTempView('songplays_table')
    songplays_table = spark.sql('''
    SELECT songplay_id AS songplay_id,
    timestamp   AS start_time,
    userId      AS user_id,
    level       AS level,
    song_id     AS song_id,
    artist_id   AS artist_id,
    sessionId   AS session_id,
    location    AS location,
    userAgent   AS user_agent
    FROM songplay_df
    ORDER BY (user_id, session_id)
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month')(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-sparkify-output"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
