# etl.py reads and processes files from song_data and log_data and loads them into created tables

import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    *Parameters*:
    cur: psycopg2 database connection
    filepath: 'data/song_data'
    
    *Process*: 
    Selected columns from song dataframe such as 'song_id', 'title', 'artist_id', 
    'year', 'duration' will be inserted into respective tables (song_table_insert, 
    artist_table_insert)
    # The tables are defined in sql_queries.py and created in created_tables.py. 
    """
    
    # To open and read the content of 'song_data'
    df = pd.read_json(filepath, lines=True)

    # To insert first song record with specified values into the song_table_insert
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # To insert first artist record with specified values into the artist_table_insert
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
   
    """
    *Parameters*:
    cur: psycopg2 database connection
    filepath: 'data/log_data'
    
    *Process*: 
    The generated contents from the log file (time_data, user_df and 
    songplay_data) will be inserted into respective tables 
    (time_table_insert, users_table_insert, songplay_table_insert)
    # The tables are defined in sql_queries.py and created in created_tables.py. 
    """
    
    # To open and read the contents of the log_data:
    df = pd.read_json(filepath, lines=True)

    # To filter records by NextSong action:
    df = df[df["page"] == "NextSong"]

    # To convert timestamp column, which is in milliseconds, to datetime:
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # To insert time data records by combining column_labels and time_data into a dictionary and converting it into a dataframe:
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, 
                 t.dt.weekday)
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # To load user table with specified values
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # To insert user records into user_table_insert
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # To insert songplay records
    for index, row in df.iterrows():
        
        # To get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # To insert songplay record into songplay_table_insert
        songplay_data = (index, pd.to_datetime(row.ts, unit="ms"), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Parameters: 
    cur: psycopg2 database connection to the cursor
    conn: psycopg2 database connection
    filepath: data ('data/song_data'; 'data/log_data')
    func: func(cur, datafile) iteratively processes every file pointed by the cursor
    
    Process:
    All files that match extension from the directory of song_data and log_data will 
    be processed by iteratively applying the function of 'func' to every datafile.
    """
    
    # To get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # To get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # To iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Process: 
    function 'main' processes the data from data/song_data' and data/log_data' by 
    utilizing the function of 'func' on every file and loading data into created 
    tables in the sparkifydb database.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()