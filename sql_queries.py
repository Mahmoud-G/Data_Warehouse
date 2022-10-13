import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events;"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs;"
songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR,
        gender          CHAR,
        itemInSession   INT,
        lastName        VARCHAR,
        length          DECIMAL,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    DECIMAL,
        sessionId       INT,
        song            VARCHAR,
        status          INT,
        ts              BIGINT,
        userAgent       VARCHAR,
        userId          INT
        );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     DECIMAL,
        artist_longitude    DECIMAL,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            DECIMAL,
        year                FLOAT
        );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY (0,1) PRIMARY KEY,
        start_time  timestamptz NOT NULL sortkey distkey,
        user_id     INT NOT NULL,
        LEVEL       VARCHAR,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  INT,
        location    VARCHAR,
        user_agent  VARCHAR
        );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
        user_id     INT PRIMARY KEY sortkey,
        first_name  VARCHAR,
        last_name   VARCHAR,
        gender      VARCHAR,
        LEVEL       VARCHAR
        );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR PRIMARY KEY sortkey,
        title       VARCHAR,
        artist_id   VARCHAR NOT NULL,
        YEAR        INT,
        duration    NUMERIC
        );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR PRIMARY KEY sortkey,
        NAME        VARCHAR,
        location    VARCHAR,
        latitude    DECIMAL,
        longitude   DECIMAL
        );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
        time_id     INT IDENTITY (0,1) PRIMARY KEY,
        start_time  timestamptz sortkey distkey,
        HOUR        INT,
        DAY         INT,
        WEEK        INT,
        MONTH       INT,
        YEAR        INT,
        weekday     VARCHAR
        );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON {}
    timeformat as 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), DWH_ROLE_ARN, config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                 SELECT DISTINCT
                    artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude                    
                 FROM staging_songs as sng 
                 join staging_events as evnt on sng.artist_name = evnt.artist
                 WHERE evnt.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
                 SELECT DISTINCT
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level                    
                 FROM staging_songs;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
                 SELECT DISTINCT
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration                    
                 FROM staging_songs; 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
                 SELECT DISTINCT
                    artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude                    
                 FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        (TO_CHAR(start_time :: DATE, 'yyyyMMDD')::integer)        AS start_time,
        EXTRACT(hour FROM start_time)                              AS hour,
        EXTRACT(day FROM start_time)                               AS day,
        EXTRACT(week FROM start_time)                              AS week,
        EXTRACT(month FROM start_time)                             AS month,
        EXTRACT(year FROM start_time)                              AS year,
        EXTRACT(week FROM start_time)                              AS weekday
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
