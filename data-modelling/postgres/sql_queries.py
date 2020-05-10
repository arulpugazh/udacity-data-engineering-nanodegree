# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
user_table_create = ("CREATE TABLE users"
                     "(user_id integer primary key not null, "
                     "first_name varchar(35), "
                     "last_name varchar(35), "
                     "gender varchar(1), "
                     "level varchar(20))")

song_table_create = ("CREATE TABLE songs("
                     "song_id varchar(18) primary key not null, "
                     "title varchar(100), "
                     "artist_id varchar(18) not null, "
                     "year integer, "
                     "duration float)")

artist_table_create = ("CREATE TABLE artists("
                       "artist_id varchar(18) primary key not null, "
                       "name varchar(100), "
                       "location varchar(100), "
                       "latitude float, "
                       "longitude float)")

time_table_create = ("CREATE TABLE time("
                     "start_time timestamp primary key not null, "
                     "hour integer, "
                     "day integer, "
                     "week integer, "
                     "month integer, "
                     "year integer, "
                     "weekday varchar(9))")

songplay_table_create = ("CREATE TABLE songplays("
                         "songplay_id serial primary key not null, "
                         "start_time timestamp references time(start_time), "
                         "user_id integer references users(user_id), "
                         "level varchar(20), "
                         "song_id varchar(18) references songs(song_id), "
                         "artist_id varchar(18) references artists(artist_id), "
                         "session_id integer, "
                         "location varchar(100), "
                         "user_agent varchar(200))")


# INSERT RECORDS

songplay_table_insert = ("INSERT INTO songplays"
                         "(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) "
                         "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")

user_table_insert = ("INSERT INTO users "
                     "(user_id, first_name, last_name, gender, level) "
                     "VALUES(%s,%s,%s,%s,%s) "
                     "ON CONFLICT (user_id) "
                     "DO UPDATE SET level = EXCLUDED.level")

song_table_insert = ("INSERT INTO songs "
                     "(song_id, title, artist_id, year, duration) "
                     "VALUES (%s, %s, %s, %s, %s) "
                     "ON CONFLICT (song_id) "
                     "DO NOTHING")

artist_table_insert = ("INSERT INTO artists "
                       "(artist_id, name, location, latitude, longitude) "
                       "VALUES (%s, %s, %s, %s, %s) "
                       "ON CONFLICT (artist_id) "
                       "DO NOTHING")


time_table_insert = ("INSERT INTO time "
                     "(start_time, hour, day, week, month, year, weekday) "
                     "VALUES (%s,%s,%s,%s,%s,%s,%s) "
                     "ON CONFLICT (start_time) "
                     "DO NOTHING")

# FIND SONGS

song_select = ("SELECT songs.song_id, artists.artist_id FROM songs "
               "JOIN artists ON songs.artist_id= artists.artist_id "
               "WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s;")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, 
                        time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, song_table_drop,user_table_drop, 
                      artist_table_drop, time_table_drop]