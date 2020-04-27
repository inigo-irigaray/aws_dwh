import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_events;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR,
        gender        VARCHAR,
        itemInSession INTEGER,
        lastName      VARCHAR,
        length        FLOAT,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  FLOAT,
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INTEGER
        )
                              """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs        INTEGER,
        artist_id        VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location  VARCHAR,
        artist_name      VARCHAR,
        song_id          VARCHAR,
        title            VARCHAR,
        duration         FLOAT,
        year             INTEGER
        )
                              """)

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id        INTEGER IDENTITY(0,1) SORTKEY,
        start_time         TIMESTAMP NOT NULL DISTKEY,
        user_id            INTEGER NOT NULL,
        level              VARCHAR,
        song_id            VARCHAR NOT NULL,
        artist_id          VARCHAR NOT NULL,
        session_id         INTEGER,
        location           VARCHAR,
        user_agent         VARCHAR
        );
                         """)

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id        INTEGER NOT NULL SORTKEY,
        first_name     VARCHAR, 
        last_name      VARCHAR,
        gender         VARCHAR,
        level          VARCHAR,
        PRIMARY KEY(user_id)
        );
                     """)

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id        VARCHAR NOT NULL SORTKEY,
        title          VARCHAR,
        artist_id      VARCHAR,
        year           INTEGER,
        duration       FLOAT,
        PRIMARY KEY(song_id)
        );
                     """)

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id    VARCHAR NOT NULL SORTKEY,
        name         VARCHAR,
        location     VARCHAR,
        latitude     FLOAT,
        longitude    FLOAT,
        PRIMARY KEY(artist_id)
        );
                       """)

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time    TIMESTAMP NOT NULL DISTKEY SORTKEY,
        hour          INTEGER,
        day           INTEGER,
        week          INTEGER,
        month         INTEGER,
        year          INTEGER,
        weekday       INTEGER,
        PRIMARY KEY(start_time)
        );
                     """)

# STAGING TABLES

staging_songs_copy = ("""
                        COPY staging_songs FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        REGION 'us-west-2' FORMAT AS JSON 'auto';
""").format(SONG_DATA, ARN)

staging_events_copy = ("""
                        COPY staging_events FROM {}
                        CREDENTIALS 'aws_iam_role={}'
                        REGION 'us-west-2' FORMAT AS JSON {}
                        TIMEFORMAT AS 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSON)

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT  DISTINCT(e.ts) AS start_time,
                e.userId AS user_id,
                e.level AS level,
                s.song_id AS song_id,
                s.artist_id AS artist_id,
                e.sessionId AS session_id,
                e.location AS location,
                e.userAgent AS user_agent
        FROM staging_events e
        JOIN staging_songs s ON (e.artist=s.artist_name AND e.song=s.title)
        WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT  e.userId AS user_id,
                e.firstName AS first_name,
                e.lastName AS last_name,
                e.gender AS gender,
                e.level AS level
        FROM staging_events e
        WHERE e.page = 'NextSong' AND e.userId IS NOT NULL
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT  s.song_id AS song_id,
                s.title AS title,
                s.artist_id AS artist_id,
                s.year AS year,
                s.duration AS duration
        FROM staging_songs s
        WHERE s.song_id IS NOT NULL
""")

artist_table_insert = ("""
        INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT  s.artist_id AS artist_id,
                s.artist_name AS artist_name,
                s.artist_location AS location,
                s.artist_latitude AS latitude,
                s.artist_longitude AS longitude
        FROM staging_songs s
        WHERE s.artist_id IS NOT NULL
""")

time_table_insert = ("""
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT  DISTINCT(s.start_time) AS start_time,
                EXTRACT(hour FROM s.start_time) AS hour,
                EXTRACT(day FROM s.start_time) AS day,
                EXTRACT(week FROM s.start_time) AS week,
                EXTRACT(month FROM s.start_time) AS month,
                EXTRACT(year FROM s.start_time) AS year,
                EXTRACT(dayofweek FROM s.start_time) AS weekday
        FROM songplay s
""")

# OPTIMIZATION QUERIES

artist_group = ("""SELECT artist_id, COUNT(artist_id) AS count FROM songplay GROUP BY (artist_id) ORDER BY count DESC LIMIT 20""")
artist_total = ("""SELECT COUNT(*) AS artist_count FROM artists""")

song_group = ("""SELECT song_id, COUNT(song_id) AS count FROM songplay GROUP BY (song_id) ORDER BY count DESC LIMIT 20""")
song_total = ("""SELECT COUNT(*) AS song_count FROM songs""")

user_group = ("""SELECT user_id, COUNT(user_id) AS count FROM songplay GROUP BY (user_id) ORDER BY count DESC LIMIT 20""")
user_total = ("""SELECT COUNT(*) AS user_count FROM users""")

time_group = ("""SELECT start_time, COUNT(start_time) AS count FROM songplay GROUP BY (start_time) ORDER BY count DESC LIMIT 20""")
time_total = ("""SELECT COUNT(*) AS time_count FROM time""")

songplay_total = ("""SELECT COUNT(*) AS total_count FROM songplay""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

optimization_queries = [artist_group, artist_total, song_group, song_total, user_group, user_total, time_group, time_total, songplay_total]
