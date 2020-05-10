# Sparkify ETL Design and Development

## Purpose of the Database

The objective is to create a database backend for songs and user activity on Sparkify's new music streaming app and build ETL pipeline for song play analysis. 

## Database Schema

Star schema is choosen for this database with one fact table and four dimension tables.

### ER Diagram

![ER Diagram](erdiagram.png) "ER Diagram of the Star Schema"

### Staging Tables

**events**: Records which are copied from S3 log json files

1. artist VARCHAR
2. auth VARCHAR
3. first_name VARCHAR
4. gender VARCHAR
5. item_in_session INTEGER
6. last_name VARCHAR
7. length FLOAT
8. level VARCHAR
9. location VARCHAR
10. method VARCHAR
11. page VARCHAR
12. registration FLOAT
13. session_id INTEGER
14. song VARCHAR
15. status INTEGER
16. ts TMESTAMP
17. user_agent VARCHAR
18. user_id INTEGER

**songs**: Records which are copied from S3 song json files
1. num_songs INTEGER
2. artist_id VARCHAR
3. artist_latitude FLOAT
4. artist_longitude FLOAT
5. artist_location VARCHAR
6. artist_name VARCHAR
7. song_id VARCHAR
8. title VARCHAR
9. duration FLOAT
10. year INTEGER


### Fact Table

**songplays**:Records in log data associated with song plays i.e. records with page NextSong

1. songplay_id serial primary key
2. start_time timestamp references time(start_time)
3. user_id integer references users(user_id)
4. level varchar(20)
5. song_id varchar(18) references songs(song_id)
6. artist_id varchar(18) references artists(artist_id)
7. session_id integer
8. location varchar(100)
9. user_agent varchar(200)

### Dimension Tables

**users**:Users in the app

1. user_id integer primary key
2. first_name varchar(35)
3. last_name varchar(35)
4. gender varchar(1)
5. level varchar(20)

**songs**:songs in music database

1. song_id varchar(18) primary key
2. title varchar(100)
3. artist_id varchar(18)
4. year integer
5. duration float

**artists** artists in music database

1. artist_id varchar(18) primary key
2. name varchar(100)
3. location varchar(100)
4. latitude float
5. longitude float

**time**:timestamps of records in songplays broken down into specific units

1. start_time timestamp primary key
2. hour integer
3. day integer
4. week integer
5. month integer
6. year integer
7. weekday varchar(9)

## How the ETL pipeline is structured

1. Establish database connection
2. Drop all tables in Redshift if they exist 
3. Create staging tables and the final tables
4. Read events information from the JSON files in S3 and copy to Redshift
5. Read songs information from the JSON files in S3 and copy to Redshift
6. Extract and transform data from staging tables and insert them into final tables in Redshift
7. Close the connection

## Usage

1. Run the script `create_tables.py`: `python create_tables.py`
2. Run the script `etl.py`: `python etl.py`