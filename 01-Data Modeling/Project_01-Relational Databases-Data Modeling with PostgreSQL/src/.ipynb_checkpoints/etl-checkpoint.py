# Import all the necessary packages
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Utility function to process one JSON file from the `song_data` directory.

    - Inserts a record into the `songs` and `artists` tables.

    Parameters
    ----------
    cur :
        Cursor object to `sparkifydb`.
    filepath : str
        Absolute path to a single data file from `song_data`.

    Returns
    -------
    None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Utility function to process one JSON file from the `log_data` directory.

    - Filters records based on the 'NextSong' page.

    - Inserts record(s) into the `time`, `users` and `songplays` tables.

    Parameters
    ----------
    cur :
        Cursor object to `sparkifydb`.
    filepath : str
        Absolute path to a single data file from `log_data`.

    Returns
    -------
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = list((t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday))
    column_labels = list(('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'))
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # NOTE: Make sure to not include 'index' as the first column as it is set to be SERIAL which is "auto-increment".
        songplay_data = (time_df['start_time'].loc[index], row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Process all the data files within a directory.

    Parameters
    ----------
    cur :
        Cursor object to `sparkifydb`.
    conn :
        Connection object to `sparkifydb`.
    filepath : str
        Absolute path to a single data file (can be from `song_data` or `log_data`).
    func :
        Function object to indicate whether to process data files from `song_data` or `log_data`.

    Returns
    -------
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.

    - Processes all the files in `song_data` and `log_data` consecutively.

    - Finally, closes the connection.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()



if __name__ == "__main__":
    main()