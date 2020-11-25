import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from app_config_reader import get_database_configuration


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filespath):
    """
    This procedure processes all song files whose filepath has been provided as an arugment.
    It load data from all files to Pandas DataFrame then
    extracts the song information in order to store it into the songs table.
    Then it extracts the artist information in order to store it into the artists table.

    INPUTS: 
    * cur the cursor variable
    * filespath list of the song files to be loaded
    """
    
    # open song file
    df = pd.DataFrame([pd.read_json(path_or_buf=f, typ='series', dtype=False) for f in filespath])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.executemany(SONG_TABLE_INSERT, song_data)
        
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_longitude', 'artist_latitude']].values.tolist()
    cur.executemany(ARTIST_TABLE_INSERT, artist_data)    


def process_log_file(cur, filespath):
    """
    This procedure processes all log file whose filepaths has been provided as an arugment.
    It load all files to Pandas DataFrame
    It extracts the time information in order to store it into the time table.
    It extracts the user information in order to store it into the users table.
    It extracts the song_play_temp data and store it into the song_play_temp table.
    Then it execute the songplay_table_insert query in order to load the songplays table.

    INPUTS: 
    * cur the cursor variable
    * filespath the file paths to the log files
    """
    # open log file
    df = pd.concat([pd.read_json(f, lines=True) for f in filespath])

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # build time data DataFrame
    time_df = pd.DataFrame(df['ts'], copy=True)
    time_df['timestamp'] = pd.to_datetime(time_df['ts'], unit='ms')
    time_df = time_df.assign(hour       = time_df['timestamp'].dt.hour, 
                             day        = time_df['timestamp'].dt.day,
                             weekofyear = time_df['timestamp'].dt.weekofyear,
                             month      = time_df['timestamp'].dt.month,
                             year       = time_df['timestamp'].dt.year,
                             weekday    = time_df['timestamp'].dt.weekday
                        )
    # Drop unwanted column timestamp
    time_df = time_df.drop(columns='timestamp')

    cur.executemany(TIME_TABLE_INSERT, time_df.values.tolist())

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    cur.executemany(USER_TABLE_INSERT, user_df.values.tolist())

    # insert songplay_temp
    temp_df = df[['ts', 'userId','level', 'song', 'length', 'artist', 'sessionId', 'location', 'userAgent']]
    cur.executemany(SONGPLAY_TEMP_TABLE_INSERT,temp_df.values.tolist())
    
    
    # insert songplay records
    cur.execute(SONGPLAY_TABLE_INSERT)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = get_files(filepath)

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    func(cur, all_files)
    conn.commit()

def main():
    cred = get_database_configuration()
    conn = psycopg2.connect(f"host={cred['host']} dbname={cred['dbname']} user={cred['username']} password={cred['password']}")
    cur = conn.cursor()

    process_data(cur, conn, filepath=cred['songs_path'], func=process_song_file)
    process_data(cur, conn, filepath=cred['log_path'], func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()