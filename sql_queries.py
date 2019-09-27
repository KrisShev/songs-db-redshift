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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist text,
                                auth text, 
                                first_name text, 
                                gender text,
                                item_in_session INTEGER, 
                                last_name text, 
                                length numeric,
                                level text, 
                                location text, 
                                method text,
                                page text,
                                registartion BIGINT,
                                session_id INTEGER,
                                song text,
                                status INTEGER,
                                ts BIGINT,
                                user_agent text,
                                user_id INTEGER)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs INTEGER,
                                artist_id VARCHAR(50),
                                artist_latitude VARCHAR(100),
                                artist_longitude VARCHAR(100),
                                artist_location VARCHAR(200),
                                artist_name VARCHAR(400),
                                song_id VARCHAR(50),
                                title VARCHAR(500),
                                duration FLOAT,
                                year INTEGER)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY NOT NULL, 
                            start_time DATE, 
                            user_id INTEGER,
                            level VARCHAR(10), 
                            song_id VARCHAR(50), 
                            artist_id VARCHAR(50), 
                            session_id INTEGER, 
                            location VARCHAR(120), 
                            user_agent VARCHAR(200)
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY NOT NULL, 
                        first_name VARCHAR(20), 
                        last_name VARCHAR(20), 
                        gender VARCHAR(1), 
                        level VARCHAR(10))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR(50) PRIMARY KEY NOT NULL, 
                        title VARCHAR(500), 
                        artist_id VARCHAR(50), 
                        year INTEGER, 
                        duration FLOAT)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                          artist_id VARCHAR(50) PRIMARY KEY NOT NULL, 
                          name VARCHAR(400), 
                          location VARCHAR(200), 
                          lattitude INTEGER, 
                          longitude INTEGER)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time DATE PRIMARY KEY NOT NULL, 
                        hour VARCHAR(2), 
                        day VARCHAR(2), 
                        week VARCHAR(2), 
                        month VARCHAR(2), 
                        year INTEGER, 
                        weekday VARCHAR(1))
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} credentials 'aws_iam_role={}' json {}
                        """).format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'] )


staging_songs_copy = ("""copy staging_songs from {}
                         credentials 'aws_iam_role={}'
                         json 'auto'
                         region 'us-west-2'
""").format(config['S3'].get('SONG_DATA'), *config["IAM_ROLE"].values())


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT DISTINCT timestamp 'epoch' + a.ts * interval '1 second'  as start_date, 
                                    user_id,
                                    level,
                                    song_id,
                                    artist_id,
                                    session_id,
                                    location,
                                    user_agent
                                    FROM staging_events a
                                    LEFT OUTER JOIN staging_songs b ON a.song=b.title and a.artist=b.artist_name and a.length=b.duration
                                    WHERE a.page='NextSong';
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id,
                            first_name,
                            last_name,
                            gender,
                            level
                        FROM staging_events 
                        WHERE page='NextSong';
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id,
                                title,
                                artist_id,
                                year,
                                duration
                        FROM staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                          SELECT DISTINCT artist_id,
                                  artist_name,
                                  artist_location,
                                  CAST(artist_latitude as FLOAT) as lattitude,
                                  CAST(artist_longitude as FLOAT) as longitude
                            FROM staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT start_time as start_date,
                                EXTRACT(hour from start_date) as hour,
                                EXTRACT(day from start_date) as day,
                                EXTRACT(week from start_date) as week,
                                EXTRACT(month from start_date) as month,
                                EXTRACT(year from start_date) as year,
                                EXTRACT(weekday from start_date) as weekday
                        FROM songplays;
                                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
