import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES IF EXIST 

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE  IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS  songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS  artists"
time_table_drop = "DROP TABLE IF EXISTS  times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR,
        item_in_session VARCHAR ,
        last_name VARCHAR,
        length float, 
        level VARCHAR,
        location VARCHAR,
        method  VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId VARCHAR,
        song  VARCHAR,
        status VARCHAR ,
        ts numeric,
        user_agent  VARCHAR,
        userId VARCHAR
    )
""")

staging_songs_table_create = ("""
 CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs numeric,
        artist_id VARCHAR,
        artist_latitude float,
        artist_longitude float,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR, 
        duration float,
        year numeric
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int GENERATED ALWAYS AS IDENTITY ,
        start_time VARCHAR NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR  NOT NULL,
        session_id VARCHAR,
        location VARCHAR, 
        user_agent VARCHAR,
        primary key(songplay_id)
    )
""")

user_table_create = ("""
 CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR NOT NULL,
        first_name VARCHAR  NOT NULL,
        last_name VARCHAR,
        gender VARCHAR,
        primary key(user_id)
    )
""")

song_table_create = ("""
     CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        artist_id VARCHAR NOT NULL,
        year numeric,
        duration float,
        primary key(song_id)
    )
""")

artist_table_create = ("""
 CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR NOT NULL,
        name VARCHAR NOT NULL,
        location VARCHAR,
        lattitude float,
        longitude float,
        primary key(artist_id)
    )
""")

time_table_create = ("""
 CREATE TABLE IF NOT EXISTS times (
        start_time TIMESTAMP NOT NULL,
        hour numeric,
        day numeric,
        week numeric,
        month numeric,
        year numeric,
        weekday numeric

    )
""")
# STAGING TABLES

staging_events_copy = ("""
 COPY staging_events
    FROM {} 
    iam_role '{}'
    JSON {}
    REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
    FROM {} 
    iam_role '{}'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time ,user_id ,level,song_id,artist_id, session_id,location,user_agent)
    select distinct
        '1970-01-01'::date + e.ts/1000 * interval '1 second' as t_start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration)
    WHERE e.page = 'NextSong'

""")

user_table_insert = ("""
INSERT INTO users(user_id,first_name ,last_name ,gender )
    select distinct
        userId,
        first_name,
        last_name,
        gender
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs(song_id,title ,artist_id ,year,duration )
    select distinct
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        lattitude,
        longitude
     )
    select 
        distinct
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO times(start_time,hour,day,week,month,year,weekday)
    Select distinct
        t_start_time
        ,EXTRACT(HOUR FROM t_start_time)  As t_hour
        ,EXTRACT(DAY FROM t_start_time)   As t_day
        ,EXTRACT(WEEK FROM t_start_time)  As t_week
        ,EXTRACT(MONTH FROM t_start_time) As t_month
        ,EXTRACT(YEAR FROM t_start_time)  As t_year
        ,EXTRACT(DOW FROM t_start_time)   As t_weekday
    FROM (
        SELECT distinct
            '1970-01-01'::date + ts/1000 * interval '1 second' as t_start_time
        FROM staging_events
        WHERE page = 'NextSong'
    ) tbl
""")

# QUERY LISTS


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
