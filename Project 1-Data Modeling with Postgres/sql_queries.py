# DROP TABLES

songplays_drop = "DROP TABLE IF EXISTS songplays"
users_drop = "DROP TABLE IF EXISTS users"
songs_drop = "DROP TABLE IF EXISTS songs"
artists_drop = "DROP TABLE IF EXISTS artists"
time_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# songplay_id is auto_incremented
# (Timestamp('2018-11-29 00:00:57.796000'), 73, 'paid', None, None, 954, 'Tampa-St. Petersburg-Clearwater, FL', '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.78.2 (KHTML, like Gecko) Version/7.0.6 Safari/537.78.2"')
# REFERENCES songs (song_id)
# REFERENCES artists (artist_id)
songplays_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id SERIAL PRIMARY KEY, 
                            start_time TIMESTAMP REFERENCES time (start_time),
                            user_id INT REFERENCES users (user_id),
                            level VARCHAR NOT NULL,
                            song_id VARCHAR,
                            artist_id VARCHAR,
                            session_id INT NOT NULL,
                            location VARCHAR,
                            user_agent TEXT)
                        """)

users_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INT PRIMARY KEY,
                        first_name VARCHAR,
                        last_name VARCHAR,
                        gender VARCHAR,
                        level VARCHAR NOT NULL)
                    """)
# only have foreign keys in fact tables
songs_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR,
                        artist_id VARCHAR,
                        year INT CHECK (year >= 0),
                        duration FLOAT CHECK (duration >= 0))
                    """)
#latitude DECIMAL(9,6)
artists_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id VARCHAR PRIMARY KEY,
                          name VARCHAR,
                          location VARCHAR,
                          latitude FLOAT,
                          longitude FLOAT)
                    """)

time_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INT NOT NULL CHECK (hour >= 0),
                        day INT NOT NULL CHECK (day >= 0),
                        week INT NOT NULL CHECK (week >= 0),
                        month INT NOT NULL CHECK (month >= 0),
                        year INT NOT NULL CHECK (year >= 0),
                        weekday VARCHAR NOT NULL)
                    """)

# INSERT RECORDS
# DEFAULT here refers to the auto_incremented songplay_id
songplays_insert = ("""INSERT INTO songplays VALUES(DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)""")

# update level
users_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE
                        SET level = EXCLUDED.level
                    """)

# do not override song
songs_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (song_id) DO NOTHING                           
                """)

artists_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (artist_id) DO UPDATE SET
                       location = EXCLUDED.location,
                       latitude = EXCLUDED.latitude,
                       longitude = EXCLUDED.longitude
                      """)


time_insert = ("""INSERT INTO time VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS
song_select = (""" SELECT song_id, artists.artist_id
                   FROM songs JOIN artists ON songs.artist_id = artists.artist_id
                   WHERE songs.title = %s
                   AND artists.name = %s
                   AND songs.duration = %s
              """)

# QUERY LISTS

# create_table_queries = [songplays_create, users_create, songs_create, artists_create, time_create]
create_table_queries = [users_create, artists_create, songs_create, time_create, songplays_create]
drop_table_queries = [songplays_drop, users_drop, songs_drop, artists_drop, time_drop]