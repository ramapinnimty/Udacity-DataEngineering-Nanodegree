# Import all the necessary packages
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay" # Fact table
# All tables below are Dimension tables
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"


# CREATE TABLES
### From Log dataset
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR, 
    auth VARCHAR, 
    firstName VARCHAR, 
    gender VARCHAR, 
    itemInSession INT, 
    lastName VARCHAR, 
    length FLOAT, 
    level VARCHAR, 
    location VARCHAR, 
    method VARCHAR, 
    page VARCHAR, 
    registration BIGINT, 
    sessionId INT, 
    song VARCHAR, 
    status INT, 
    ts TIMESTAMP, 
    userAgent VARCHAR, 
    userId INT);
""")

### From Song dataset
'''
    {
        "num_songs": 1, 
        "artist_id": "ARJIE2Y1187B994AB7", 
        "artist_latitude": null, 
        "artist_longitude": null, 
        "artist_location": "", 
        "artist_name": "Line Renaud", 
        "song_id": "SOUPIRU12A6D4FA1E1", 
        "title": "Der Kleine Dompfaff", 
        "duration": 152.92036, 
        "year": 0
    }
'''
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT, 
    artist_id VARCHAR, 
    artist_latitude FLOAT, 
    artist_longitude FLOAT, 
    artist_location VARCHAR, 
    artist_name VARCHAR, 
    song_id VARCHAR, 
    title VARCHAR, 
    duration FLOAT, 
    year INT);
""")

# `SERIAL` alternative for Amazon Redshift: `IDENTITY(0, 1)` from https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time TIMESTAMP NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR, 
    song_id VARCHAR, 
    artist_id VARCHAR, 
    session_id INT, 
    location VARCHAR, 
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_id INT PRIMARY KEY distkey, 
    first_name VARCHAR, 
    last_name VARCHAR, 
    gender VARCHAR,
    level VARCHAR);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR distkey, 
    year INT, 
    duration FLOAT NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id VARCHAR PRIMARY KEY distkey, 
    name VARCHAR NOT NULL, 
    location VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY sortkey distkey, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT);
""")


# STAGING TABLES (Ingest data at "scale" using COPY)
# Useful resources: 
    # https://knowledge.udacity.com/questions/98859
    # https://knowledge.udacity.com/questions/760058
    # https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-load.html#copy-compupdate

# Event data location: s3://udacity-dend/log_data
# File for 'mapping' Event data: s3://udacity-dend/log_json_path.json
staging_events_copy = ("""
COPY staging_events
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH')
)

# Song data location: s3://udacity-dend/song_data
staging_songs_copy = ("""
COPY staging_songs
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN')
)


# FINAL TABLES
# Useful resource: postgresqltutorial.com/postgresql-string-functions/postgresql-to_char/
songplay_table_insert = ("""
    INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TO_TIMESTAMP(TO_CHAR(se.ts, '9999-99-99 99:99:99'), 'YYYY-MM-DD HH24:MI:SS'), 
                se.userId as user_id, 
                se.level as level, 
                ss.song_id as song_id, 
                ss.artist_id as artist_id, 
                se.sessionId as session_id, 
                se.location as location, 
                se.userAgent as user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song=ss.title AND se.artist=ss.artist_name;
""")

user_table_insert = ("""
    INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id, 
        firstName AS first_name, 
        lastName AS last_name, 
        gender AS gender, 
        level AS level
    FROM staging_events
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO dim_song (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id AS song_id, 
        title AS title, 
        artist_id AS artist_id, 
        year AS year, 
        duration AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id AS artist_id, 
        artist_name AS name, 
        artist_location AS location, 
        artist_latitude AS latitude, 
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT ts, 
        EXTRACT(hour FROM ts),
        EXTRACT(day FROM ts),
        EXTRACT(week FROM ts),
        EXTRACT(month FROM ts),
        EXTRACT(year FROM ts),
        EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries   = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]