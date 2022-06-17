# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES

###   References:   ###
#         - https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/
#         - https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-data-types/
#         - https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/


#----- Sample file at `data/log_data/2018/11/2018-11-01-events.json` -----#
# {
#     "artist": null,
#     "auth": "Logged In",
#     "firstName": "Walter",
#     "gender": "M",
#     "itemInSession": 0,
#     "lastName": "Frye",
#     "length": null,
#     "level": "free",
#     "location": "San Francisco-Oakland-Hayward, CA",
#     "method": "GET",
#     "page": "Home",
#     "registration": 1540919166796.0,
#     "sessionId": 38,
#     "song": null,
#     "status": 200,
#     "ts": 1541105830796,
#     "userAgent": "\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
#     "userId": "39"
# }


# ''' From instructions: records in log data associated with song plays i.e. records with page NextSong '''
# From `test.ipynb`, for 'songplays' table: -
#     - Initially used `REAL` data type for both 'latitude' and 'longitude' columns.
#     - Check the columns 'start_time' and 'user_id' for correct data type. Check columns 'start_time' and 'user_id' for not-NULL constraint.
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY, 
    start_time TIMESTAMP NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR(20), 
    song_id VARCHAR(30), 
    artist_id VARCHAR(30), 
    session_id INT, 
    location VARCHAR(200), 
    user_agent VARCHAR);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY, 
    first_name VARCHAR(100), 
    last_name VARCHAR(100), 
    gender CHAR(10),
    level VARCHAR(20));
""")


#----- Sample file at `data/song_data/A/A/A/TRAAAAW128F429D538.json` -----#
# num_songs:1
# artist_id:"ARD7TVE1187B99BFB1"
# artist_latitude:null
# artist_longitude:null
# artist_location:"California - LA"
# artist_name:"Casual"
# song_id:"SOMZWCG12A8C13C480"
# title:"I Didn't Mean To"
# duration:218.93179
# year:0


# From `test.ipynb`, for 'songs' table: -
#     - Check the column 'year' for correct data type. Check columns 'title' and 'duration' for not-NULL constraints.
#     - Took the reviewer feedback into consideration and changed the datatype for 'duration' from `REAL` to `FLOAT` with 5 decimal places.
song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(30) PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR(30), 
    year INT, 
    duration FLOAT(5) NOT NULL);
""")

# From `test.ipynb`, for 'artists' table: -
#     - Initially used `REAL` & NUMERIC(8, 8) data type for both 'latitude' and 'longitude' columns.
#     - Check the columns 'latitude' and 'longitude' for correct data type. Check column 'name' for not-NULL constraint.
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(30) PRIMARY KEY, 
    name VARCHAR(100) NOT NULL, 
    location VARCHAR(200), 
    latitude DOUBLE PRECISION, 
    longitude DOUBLE PRECISION);
""")

# ''' From instructions: timestamps of records in songplays broken down into specific units '''
#----- Sample file at `data/log_data/2018/11/2018-11-01-events.json` -----#
# ts: 1541105830796
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT);
""")


# INSERT RECORDS

#     - Took the reviewer feedback into consideration and removed the autoincrement column `songplay_id` from the INSERT statement.
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

# From Knowledge: https://knowledge.udacity.com/questions/804945
# Intuition: - A customer currently on free-tier might convert to a paid subscriber in future!
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")

# Encountered the error: -
#     psycopg2.IntegrityError: duplicate key value violates unique constraint "time_pkey"
#     DETAIL:  Key (start_time)=(2018-11-23 14:41:51.796) already exists.
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING;
""")


# FIND SONGS

# From `etl.ipynb`, for 'songplays' table: -
#     - Implement the 'song_select' query in `sql_queries.py` to find the song ID and artist ID based on the title, artist name, and duration of a song.
song_select = ("""
SELECT s.song_id, a.artist_id
FROM songs s
JOIN artists a ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]