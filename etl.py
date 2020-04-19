import os
import glob
import pandas as pd
import sql_queries as sql


def process_song_file(cursor, filepath):
    """
    Process Song File.
    Function to process a single data song_file and send insert statements
    through cursor. To use as func in process_data function.
    Args:
        - cursor: cursor object to connected DB.
        - filepath: stirng with path to song_file to process.
    """
    # open song file
    df = pd.read_json(filepath, lines=True, encoding="ascii")

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year",
                   "duration"]].values.tolist()[0]
    song_data = [data if not pd.isnull(data) else None for data in song_data]
    sql.execute(sql.SONGS_INSERT, cursor, song_data)

    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location",
                     "artist_latitude", "artist_longitude"]].values.tolist()[0]
    artist_data = [data if not pd.isnull(data) else None
                   for data in artist_data]
    sql.execute(sql.ARTISTS_INSERT, cursor, artist_data)


def process_log_file(cursor, filepath):
    """
    Process Log File.
    Function to process a single data log_file and send insert statements
    through cursor. To use as func in process_data function.
    Args:
        - cursor: cursor object to connected DB.
        - filepath: stirng with path to log_file to process.
    """
    # open log file
    df = pd.read_json(filepath, lines=True, encoding="ascii")

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    df["ts"] = pd.to_datetime(df["ts"], unit="ms")
    t = df["ts"]

    # insert time data records
    time_data = (
        t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = (
        "start_time", "hour", "day", "week", "month", "year", "weekday")

    time_df = pd.concat(time_data, axis=1, sort=False, ignore_index=True)
    time_df.rename(
        columns={i: col for i, col in enumerate(column_labels)}, inplace=True)

    for i, row in time_df.iterrows():
        sql.execute(sql.TIME_INSERT, cursor, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].copy()
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        sql.execute(sql.USERS_INSERT, cursor, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        succ = sql.execute(
            sql.SONG_SELECT, cursor, (row.song, row.artist, row.length))
        if succ:
            results = cursor.fetchone()
        else:
            results = None
        # If ids not found, song/artist is not in dimension tables
        songid, artistid = results if results else None, None

        # insert songplay record
        songplay_data = (
            row.ts, row.userId, songid, artistid, row.sessionId, row.length,
            row.location, row.userAgent)
        sql.execute(sql.SONGPLAYS_INSERT, cursor, songplay_data)


def process_data(cursor, connection, filepath, func):
    """
    Process data.
    Function which processes data from the given path with the given function,
    which sends transactions to the given conenction through the cursor.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
        - filepath: string with file to data to process.
        - func: function to process data with.
    """
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cursor, datafile)
        connection.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    Main function.
    Main function to execute when module is called through command line.
    Connects to sparkifydb and get cursor to insert log_file and song_file
    data.
    """
    cursor, connection = sql.connect(database="sparkifydb", autocommit=False)

    process_data(
        cursor, connection, filepath='data/song_data', func=process_song_file)
    process_data(
        cursor, connection, filepath='data/log_data', func=process_log_file)

    sql.disconnect(cursor, connection)


if __name__ == "__main__":
    main()
