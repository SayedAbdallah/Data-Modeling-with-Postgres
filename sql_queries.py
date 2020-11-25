# DROP TABLES
SONG_TABLE_DROP     = "DROP TABLE IF EXISTS songs;"
ARTIST_TABLE_DROP   = "DROP TABLE IF EXISTS artists;"
USER_TABLE_DROP     = "DROP TABLE IF EXISTS users;"
TIME_TABLE_DROP     = "DROP TABLE IF EXISTS time;"
SONGPLAY_TABLE_DROP = "DROP TABLE IF EXISTS songplays;"
SONGPLAY_TEMP_TABLE_DROP = "DROP TABLE IF EXISTS songplays_temp;"

# CREATE TABLES

SONG_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR,
    title VARCHAR       NOT NULL,
    artist_id VARCHAR   NOT NULL,
    year INT            NOT NULL,
    duration NUMERIC    NOT NULL,
    CONSTRAINT SONG_PK PRIMARY KEY (song_id)
);
""")

ARTIST_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR,
    name VARCHAR        NOT NULL,
    location VARCHAR    NULL, 
    latitude NUMERIC    NULL, 
    longitude NUMERIC   NULL,
    CONSTRAINT ARTIST_PK PRIMARY KEY (artist_id)
);
""")

USER_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id INT, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR  NOT NULL, 
    gender VARCHAR     NOT NULL, 
    level VARCHAR      NOT NULL,
    CONSTRAINT USER_PK PRIMARY KEY (user_id)
);
""")

TIME_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time NUMERIC,
    hour INT               NOT NULL,
    day INT                NOT NULL,
    week INT               NOT NULL,
    month INT              NOT NULL,
    year INT               NOT NULL,
    weekday VARCHAR        NOT NULL,
    CONSTRAINT TIME_PK PRIMARY KEY (start_time)
);
""")

SONGPLAY_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id SERIAL,
    start_time NUMERIC  NOT NULL,
    user_id VARCHAR     NOT NULL,
    level VARCHAR       NOT NULL,
    song_id VARCHAR     NULL,
    artist_id VARCHAR   NULL,
    session_id INT      NOT NULL,
    location VARCHAR    NOT NULL,
    user_agent VARCHAR  NOT NULL,
    CONSTRAINT songplays_PK PRIMARY KEY(songplay_id)
);
""")


SONGPLAY_TEMP_TABLE_CREATE = ("""
CREATE TABLE IF NOT EXISTS songplays_temp
(
    ts NUMERIC  NOT NULL,
    user_id VARCHAR     NOT NULL,
    level VARCHAR       NOT NULL,
    song_title VARCHAR  NULL,
    song_length NUMERIC NULL,
    artist_name VARCHAR NULL,
    session_id INT      NOT NULL,
    location VARCHAR    NOT NULL,
    user_agent VARCHAR  NOT NULL
);
""")


# INSERT RECORDS

SONG_TABLE_INSERT = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING;
""")

ARTIST_TABLE_INSERT = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING;
""")

USER_TABLE_INSERT = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
""")

TIME_TABLE_INSERT = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO NOTHING;
""")

SONGPLAY_TEMP_TABLE_INSERT = ("""
INSERT INTO songplays_temp(ts, user_id, level, song_title, song_length, artist_name, session_id, location, user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);
""")

SONGPLAY_TABLE_INSERT = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT temp.ts, temp.user_id, temp.level, s.song_id, a.artist_id, temp.session_id, temp.location, temp.user_agent
FROM songplays_temp temp LEFT JOIN artists a ON temp.artist_name = a.name 
LEFT JOIN songs s on temp.song_title = s.title and temp.song_length = s.duration
"""

# QUERY LISTS

CREATE_TABLE_QUERIES = [SONG_TABLE_CREATE, ARTIST_TABLE_CREATE, TIME_TABLE_CREATE, USER_TABLE_CREATE, SONGPLAY_TEMP_TABLE_CREATE, SONGPLAY_TABLE_CREATE]
DROP_TABLE_QUERIES = [SONG_TABLE_DROP, ARTIST_TABLE_DROP, TIME_TABLE_DROP, USER_TABLE_DROP, SONGPLAY_TEMP_TABLE_DROP, SONGPLAY_TABLE_DROP]

