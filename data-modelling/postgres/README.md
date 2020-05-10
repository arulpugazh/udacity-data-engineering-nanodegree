# Sparkify ETL Design and Development

## Purpose of the Database

The objective is to create a database backend for songs and user activity on Sparkify's new music streaming app and build ETL pipeline for song play analysis. 

## Database Schema

Star schema is choosen for this database with one fact table and four dimension tables.

### ER Diagram

![ER Diagram](erdiagram.png) "ER Diagram of the Star Schema"

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
2. Read songs and artists information from the JSON files in `data/songs` directory.
3. Insert the processed data in songs table and artists table
4. Read time information information from the JSON files in `data/logs` directory.
5. Insert the time data into time, users and songplays table
6. Close the connection

## Usage

1. Run the script `create_tables.py`: `python create_tables.py`
2. Run the script `etl.py`: `python etl.py`
3. Check the results in the notebook `test.ipynb`

## Sample Query

This query retrieves the most listened artists:
`SELECT artist_id FROM songplays GROUP BY artist_id ORDER BY artist_id desc;`

**artist_id**

None

AR5KOSW1187FB35FF4

It appears most of the songs in the database doesn't have artists updated