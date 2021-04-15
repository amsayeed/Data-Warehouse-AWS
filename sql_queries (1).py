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

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR, 
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length double precision,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration DOUBLE PRECISION,
sessionId INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
user_id VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
song_id VARCHAR,
num_songs INTEGER,
title VARCHAR,
artist_name VARCHAR,
artist_latitude DOUBLE PRECISION,
year INTEGER,
duration DOUBLE PRECISION,
artist_id VARCHAR,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey,
start_time TIMESTAMP NOT NULL,
user_id INTEGER NOT NULL,
level VARCHAR,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL,
session_id INTEGER,
location VARCHAR,
user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
user_id INTEGER PRIMARY KEY distkey,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id VARCHAR PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR distkey,
year INTEGER,
duration DOUBLE PRECISION
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
artist_id VARCHAR PRIMARY KEY distkey,
name VARCHAR,
location VARCHAR,
latitude DOUBLE PRECISION,
longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY sortkey distkey,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
JSON {}
REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))


staging_songs_copy = ("""
COPY staging_songs
FROM {} 
IAM_ROLE {}
JSON 'auto'
REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
    e.user_id::INT,
    e.level,
    s.song_id,
    e.artist,
    e.sessionId,
    e.location,
    e.userAgent    
FROM staging_events e
LEFT JOIN staging_songs s
ON e.song = s.title
AND e.artist = s.artist_id
WHERE page = 'NextSong' and s.song_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT user_id::INT as user_id,
                firstName as first_name,
                lastName as last_name,
                gender as gender,
                level as level
FROM staging_events
where user_id IS NOT NULL and page='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id as song_id,
                title as title,
                artist_id as artist_id,
                year as year,
                duration as duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id as artist_id,
                artist_name as name,
                artist_location as location,
                artist_latitude as latitude,
                artist_longitude as longitude
FROM staging_songs
where artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second',
                EXTRACT(hour from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second'),
                EXTRACT(day from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second'),
                EXTRACT(week from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second'),
                EXTRACT(month from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second'),
                EXTRACT(year from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second'),
                EXTRACT(weekday from TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second')
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = {'staging_events':staging_events_copy ,'staging_songs':staging_songs_copy } 
insert_table_queries = {'songplays':songplay_table_insert, 'users' : user_table_insert,'songs': song_table_insert,'artists': artist_table_insert,'time': time_table_insert}
