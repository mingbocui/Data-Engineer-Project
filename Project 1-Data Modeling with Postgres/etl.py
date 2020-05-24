import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Process all song files to insert the data to the table
    """
    df = pd.DataFrame([pd.read_json(filepath, typ='series', convert_dates=False)])

    
    # get the values of the dataframe, each row will contains
    for row_value in df.values:
        num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year = row_value
        
        # insert song record
        song_data = (song_id, title, artist_id, year, duration)
        cur.execute(songs_insert, song_data)
    
        # insert artist record
        artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        cur.execute(artists_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Process all log files to insert the data to the table
    """
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df = df.astype({'ts': 'datetime64[ms]'})
    t = pd.Series(df['ts'], index=df.index)
    
    # insert time data records
    time_data = []
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    # time_df = []
    for time in t:
        time_data.append([time, time.hour, time.day, time.weekofyear, time.month, time.year, time.day_name()])
    # creat dataframe from list 
    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)
    
    for i, row in time_df.iterrows():
        cur.execute(time_insert, list(row))

    # load user table
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_columns]

    # insert user records
    # list(row)
    for i, row in user_df.iterrows():
        cur.execute(users_insert, list(row))

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
        songplay_data = (row.ts, int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplays_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Walk through the folder and get all files path
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()