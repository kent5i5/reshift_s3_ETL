import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar(200) ,
    auth varchar(200) ,
    first_name varchar(200) , 
    gender varchar(25)  , 
    itemInSession int ,
    last_name varchar(200) , 
    length float,
    level varchar(25),
    location varchar(200),
    method varchar(100),
    page varchar(25),
    registration bigint,
    sessionId int ,
    song varchar(200),
    status int,
    ts bigint,
    userAgent varchar(200),
    user_id varchar(200) );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar(100) not null ,
        artist_latitude float,
        artist_location varchar(100),
        artist_longitude float,
        artist_name varchar(100) not null,
        duration float not null,
        num_songs INTEGER not null ,
        song_id varchar(200) not null,
        title varchar(200) ,
        year smallint not null);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimUser(
    user_id varchar(200) , 
    first_name varchar(200)  , 
    last_name varchar(200)  , 
    gender varchar(25)  , 
    level varchar(25),  
    PRIMARY KEY(user_id));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSong(
    song_id bigint identity(0,1), 
    title varchar(25) not null, 
    artist_id integer not null, 
    year varchar(25) not null, 
    duration float not null,
    PRIMARY KEY(song_id))
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimArtist(
    artist_id varchar(100) , 
    name varchar(100) NOT NULL, 
    location varchar(100) , 
    latitude float, 
    longtitude float,
    PRIMARY KEY(artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS  dimTime( 
    start_time timestamp, 
    hour smallint  not null, 
    day smallint not null, 
    week smallint not null, 
    month smallint not null, 
    year smallint not null, 
    weekday varchar(19)  not null,
    PRIMARY KEY(start_time));
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id bigint identity(0,1), 
    start_time timestamp not null, 
    user_id bigint not null, 
    level varchar(10) not null, 
    song_id bigint not null, 
    artist_id bigint not null, 
    session_id bigint not null, 
    location varchar(100), 
    user_agent varchar(25) not null,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) references dimUser(user_id), 
    FOREIGN KEY(artist_id) references dimArtist(artist_id), 
    FOREIGN KEY(song_id) references dimSong(song_id), 
    FOREIGN KEY(start_time) references dimTime(start_time));
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data/2018/11/2018' 
credentials 'aws_iam_role=arn:aws:iam::168180329753:role/redshit_s3'
JSON 's3://udacity-dend/log_json_path.json'
region 'us-west-2';
""")

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data/A/A/A/TRA' 
credentials 'aws_iam_role=arn:aws:iam::168180329753:role/redshit_s3'
format as JSON 'auto'
region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)

SELECT '1970-01-01'::date + ts/1000 * interval '1 second' as ts_datetime,user_id,level,song_id,artist_id,sessionId,location,userAgent

FROM (

SELECT se.ts, se.user_id, se.level, sa.song_id, sa.artist_id, se.sessionId, se.location, se.userAgent

FROM staging_events AS se

JOIN

(SELECT dimsong.song_id, dimartist.artist_id, dimsong.title, dimartist.name,dimsong.duration

FROM dimsong

JOIN dimartist

ON dimsong.artist_id = dimartist.artist_id) AS sa

ON (sa.title = se.song

AND sa.name = se.artist

AND sa.duration = se.length)

WHERE se.page = 'NextSong');
""")

user_table_insert = ("""
INSERT INTO dimUser(user_id, first_name, last_name, gender , level)
select user_id, first_name, last_name, gender , level  from staging_events;
""")

song_table_insert = ("""
INSERT INTO dimSong(song_id, title, artist_id , year, duration)
select song_id, title, artist_id , year, duration from staging_songs;
""")

artist_table_insert = ("""
INSERT INTO dimArtist(artist_id, name, location, latitude, longitude)
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs;

""")

# time_table_insert = ("""
# INSERT INTO () VALUES ()
# """)

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,  
                        user_table_create, song_table_create, artist_table_create,
                        time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    #songplay_table_insert, 
    user_table_insert, song_table_insert, artist_table_insert, 
    #time_table_insert
]
