import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# NOT NULL constraints should be used where applicable. Recommended to use it on columns acting as foreign keys to ensure they are not filled with nulls. 
# You can handle the null values by adding an # filtering for NULL values on NOT NULL columns prior to insertion so that you receive no errors while insertion due to the NO NULL constraint
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                 artist VARCHAR,
                                 auth VARCHAR,
                                 firstName VARCHAR,
                                 gender CHAR,
                                 itemInSession INTEGER,
                                 lastName VARCHAR,
                                 length FLOAT,
                                 level VARCHAR,
                                 location VARCHAR,
                                 method VARCHAR,
                                 page VARCHAR,
                                 registration FLOAT,
                                 sessionId INTEGER,
                                 song VARCHAR,
                                 status INTEGER,
                                 ts INTEGER,
                                 userAgent VARCHAR,
                                 userId INTEGER)
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                 num_songs INTEGER,
                                 artist_id VARCHAR,
                                 artist_latitude FLOAT,
                                 artist_longitude FLOAT,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR,
                                 title VARCHAR,
                                 duration FLOAT,
                                 year FLOAT)
                             """)
# use key distribution
# refer to some encodings information
# https://docs.aws.amazon.com/redshift/latest/dg/Examples__compression_encodings_in_CREATE_TABLE_statements.html
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INTEGER IDENTITY (1, 1) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id INTEGER NOT NULL,
                            level VARCHAR,
                            song_id VARCHAR NOT NULL,
                            artist_id VARCHAR NOT NULL,
                            session_id VARCHAR,
                            location VARCHAR,
                            userAgent VARCHAR)
                            DISTSTYLE KEY
                            DISTKEY (start_time)
                            SORTKEY (start_time)
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY,
                        firstName VARCHAR,
                        lastName VARCHAR,
                        gender CHAR,
                        level VARCHAR)
                        SORTKEY (user_id)
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR,
                        artist_id VARCHAR,
                        year INTEGER,
                        duration FLOAT)
                        SORTKEY (song_id)
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id VARCHAR PRIMARY KEY,
                          name VARCHAR,
                          location VARCHAR,
                          latitude FLOAT,
                          longitude FLOAT)
                          SORTKEY (artist_id)
                      """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INTEGER,
                        day INTEGER,
                        week INTEGER,
                        month INTEGER,
                        year INTEGER,
                        weekday VARCHAR(9))
                        DISTSTYLE KEY
                        DISTKEY (start_time)
                        SORTKEY (start_time);
                    """)

# STAGING TABLES

staging_events_copy = ("""COPY staging_events
                          FROM {}
                          iam_role {}
                          FORMAT AS json 'auto';
                          """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""COPY staging_songs
                         FROM {}
                         iam_role {}
                         FORMAT AS json 'auto';
                      """).format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES
# Redshift doesnt have the from_unixtime() function. You'll need to use the below sql query to get the timestamp. It just adds the no of seconds to epoch and return as timestamp.
# https://stackoverflow.com/questions/39815425/how-to-convert-epoch-to-datetime-redshift
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, userAgent)
                            SELECT DISTINCT TIMESTAMP 'epoch' + (ste.ts/1000) * INTERVAL '1 second' as start_time,
                                            ste.userId,
                                            ste.level,
                                            sts.song_id,
                                            sts.artist_id,
                                            ste.sessionId,
                                            ste.location,
                                            ste.userAgent
                            FROM staging_songs sts
                            JOIN staging_events ste
                            ON sts.title = ste.song AND ste.artist = sts.artist_name AND ste.page = 'NextSong'
                         """)

user_table_insert = ("""INSERT INTO users (user_id, firstName, lastName, gender, level)
                        SELECT DISTINCT userId, firstName, lastName, gender, level
                        FROM staging_events ste
                        WHERE ste.page = 'NextSong'
                        AND ste.userId IS NOT NULL
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs sts
                        WHERE sts.song_id IS NOT NULL
                     """)


artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs
                        """)

# FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM songplays) a
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT TIMESTAMP 'epoch' + (ste.ts/1000) * INTERVAL '1 second' as start_time,
                                        EXTRACT(HOUR FROM start_time) AS hour,
                                        EXTRACT(DAY FROM start_time) AS day,
                                        EXTRACT(WEEKS FROM start_time) AS week,
                                        EXTRACT(MONTH FROM start_time) AS month,
                                        EXTRACT(YEAR FROM start_time) AS year,
                                        to_char(start_time, 'Day') AS weekday
                        FROM staging_events ste
                        WHERE ste.ts IS NOT NULL
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
