import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('../../dwh.cfg')

# <editor-fold desc="DROP TABLES">
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays  cascade;"
user_table_drop = "DROP TABLE IF EXISTS users cascade;"
song_table_drop = "DROP TABLE IF EXISTS songs cascade;"
artist_table_drop = "DROP TABLE IF EXISTS artists cascade;"
time_table_drop = "DROP TABLE IF EXISTS times cascade;"
# </editor-fold>

# <editor-fold desc="CREATE TABLES">
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist          VARCHAR(MAX),
auth            VARCHAR(MAX),
first_name      VARCHAR(MAX),
gender          CHAR(1),
item_in_session INTEGER,
last_name       VARCHAR(MAX),
length          DECIMAL,
level           VARCHAR,
location        VARCHAR(MAX),
method          VARCHAR(MAX),
page            VARCHAR(300),
registration    NUMERIC,
session_id      INTEGER,
song            VARCHAR(MAX),
status          INTEGER,
ts              BIGINT,
user_agent      VARCHAR(MAX),
user_id         INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs         INTEGER,
artist_id         VARCHAR(MAX),
artist_latitude   NUMERIC,
artist_longitude  NUMERIC,
artist_location   VARCHAR(MAX),
artist_name       VARCHAR(MAX),
song_id           VARCHAR(MAX),
title             VARCHAR(MAX),
duration          DECIMAL,
year              INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id            INTEGER IDENTITY ( 0,1) NOT NULL ,
start_time             timestamp  NOT NULL SORTKEY DISTKEY,
user_id                INTEGER ,
level                  VARCHAR(50),
song_id                VARCHAR(MAX),
artist_id              VARCHAR(MAX),
session_id             INTEGER ,
location               VARCHAR(MAX),
user_agent             VARCHAR(MAX),
PRIMARY KEY (songplay_id),
CONSTRAINT fk_start_time_log FOREIGN KEY  (start_time) REFERENCES times(start_time)  ,
CONSTRAINT fk_user_log       FOREIGN KEY  (user_id)    REFERENCES users(user_id)     ,
CONSTRAINT fk_song_log       FOREIGN KEY  (song_id)    REFERENCES songs(song_id)     ,
CONSTRAINT fk_artist_log     FOREIGN KEY  (artist_id)  REFERENCES artists(artist_id) 
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS  users(
user_id      INTEGER NOT NULL  SORTKEY, 
first_name   VARCHAR(MAX) NOT NULL,  
last_name    VARCHAR(MAX) NOT NULL, 
gender       CHAR(1),
level        VARCHAR(50),
PRIMARY KEY (user_id)
)diststyle ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS  songs(
song_id     VARCHAR(MAX) NOT NULL sortkey,
title       VARCHAR(MAX) NOT NULL ,
artist_id   VARCHAR(MAX) NOT NULL,
year        INTEGER ,
duration    decimal,
PRIMARY KEY (song_id),
CONSTRAINT fk_artist_song FOREIGN KEY(artist_id) REFERENCES artists(artist_id) 
)diststyle ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS  artists (
artist_id   VARCHAR(MAX) NOT NULL sortkey,
name        VARCHAR(MAX) NOT NULL,
location    VARCHAR(MAX) , 
lattitude   NUMERIC , 
longitude   NUMERIC ,
PRIMARY KEY (artist_id)
) diststyle ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS  times (
start_time      TIMESTAMP NOT NULL  sortkey DISTKEY,
hour            INTEGER NOT NULL,
day             INTEGER NOT NULL, 
week            INTEGER NOT NULL, 
month           INTEGER NOT NULL, 
year            INTEGER NOT NULL, 
weekday         INTEGER NOT NULL,
PRIMARY KEY (start_time)
);
""")
# </editor-fold>

# <editor-fold desc="STAGING TABLES">
staging_events_copy_test = ("""
    copy staging_events from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA_SAMPLE'], role_arn=config['IAM_ROLE']['ARN'],
            log_json_path=config['S3']['LOG_JSONPATH'])

staging_events_copy = ("""
    COPY staging_events FROM {} 
    credentials 'aws_iam_role={}'  
    REGION '{}' JSON {};
""").format(config['S3']['log_data'], config['IAM_ROLE']['arn'], 'us-west-2', config['S3']['log_jsonpath'])

staging_songs_copy = ("""COPY staging_songs FROM {} credentials 'aws_iam_role={}'  REGION '{}' JSON 'auto'  ;
""").format(config['S3']['song_data'], config['IAM_ROLE']['arn'], 'us-west-2')
# </editor-fold>

# <editor-fold desc="FINAL TABLES">
songplay_table_insert = ("""INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT timestamp 'epoch' + ts /1000 * interval '1 second',
a.user_id,
a.level,
e.song_id,
e.artist_id,
a.session_id,
a.location,
a.user_agent
FROM staging_events a left join staging_songs e on a.song = e.title and a.artist = e.artist_name
where a.page='NextSong'
""")

user_table_insert = ("""INSERT INTO users select  DISTINCT user_id ,first_name,last_name,gender,level 
FROM staging_events WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs SELECT DISTINCT song_id ,title,artist_id,cast(year as int),cast (duration as decimal)  FROM staging_songs WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists SELECT DISTINCT artist_id,artist_name ,artist_location ,artist_latitude ,artist_longitude  FROM staging_songs WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO times select DISTINCT
timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second',
EXTRACT(HOUR FROM timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second'),
           EXTRACT(DAY FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
           EXTRACT(WEEK FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
           EXTRACT(MONTH FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
           EXTRACT(YEAR FROM timestamp 'epoch' + ts/1000 * interval '1 second'),
           EXTRACT(DOW FROM timestamp 'epoch' + ts/1000 * interval '1 second')+1
           FROM staging_events
           WHERE ts IS NOT NULL;
""")
# </editor-fold>

# <editor-fold desc="COUNT_RECS">
get_number_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

get_number_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

get_number_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

get_number_users = ("""
    SELECT COUNT(*) FROM users
""")

get_number_songs = ("""
    SELECT COUNT(*) FROM songs
""")

get_number_artists = ("""
    SELECT COUNT(*) FROM artists
""")

get_number_times = ("""
    SELECT COUNT(*) FROM times
""")
# </editor-fold

# <editor-fold desc="QUERY LISTS">
# SETUP
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create,
                        song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
# PAYLOAD
copy_table_queries = [staging_events_copy, staging_songs_copy]
# copy_table_queries = [staging_events_copy_test] # for testing
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert, user_table_insert,
                        songplay_table_insert]
# TEST
select_number_rows_queries = [get_number_staging_events, get_number_staging_songs, get_number_songplays,
                              get_number_users, get_number_songs, get_number_artists, get_number_times]
# </editor-fold>
