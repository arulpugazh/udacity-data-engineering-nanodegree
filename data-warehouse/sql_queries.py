import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ('''
CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER)
''')

staging_songs_table_create = ('''
CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER)
''')

user_table_create = ('''
CREATE TABLE users
(user_id integer primary key not null, 
first_name varchar(35), 
last_name varchar(35), 
gender varchar(1), 
level varchar(20))''')

song_table_create = ('''
CREATE TABLE songs(
song_id varchar(18) primary key not null, 
title varchar (250), 
artist_id varchar(18) not null, 
year integer, 
duration float)''')

artist_table_create = ('''
CREATE TABLE artists(
artist_id varchar(18) primary key not null, 
name varchar (250), 
location varchar (250), 
latitude float, 
longitude float)''')

time_table_create = ('''
CREATE TABLE time(
start_time timestamp primary key not null, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday varchar(9))''')

songplay_table_create = ('''
CREATE TABLE songplays(
songplay_id integer identity(0,1) primary key not null,
start_time timestamp references time(start_time),
user_id integer references users(user_id),
level varchar(20),
song_id varchar(18) references songs(song_id),
artist_id varchar(18) references artists(artist_id),
session_id integer,
location varchar (250),
user_agent varchar(200))''')

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'],
           role_arn=config['IAM_ROLE']['ARN'],
           log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'],
           role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (
'''
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts as start_time, 
e.userId as user_id,
e.level as level,
s.song_id as song_id,
s.artist_id as artist_id,
e.sessionId as session_id,
e.location as location,
e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
AND e.page  =  'NextSong'
''')

user_table_insert = ('''
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userId) AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
FROM staging_events
WHERE user_id IS NOT NULL AND page = 'NextSong'
''')

song_table_insert = ('''
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT(song_id) as song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
''')

artist_table_insert = (
'''
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id) as artist_id,
artist_name as name,
artist_location as location,
artist_latitude as latitude,
artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
''')


time_table_insert = (
'''
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(ts) as start_time, 
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
EXTRACT(dayofweek FROM start_time) AS weekday
FROM staging_events
''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]