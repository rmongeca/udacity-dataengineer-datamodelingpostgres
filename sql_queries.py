"""
SQL Queries Script.
This script contains the queries that are executed in the other scripts,
together with wrapper functions around the psycopg2 functions to handle the DB
which control error exceptions.
"""
import psycopg2

# Creation table queries
SONGPLAYS_CREATION = """
CREATE TABLE songplays (
    songplay_id serial PRIMARY KEY,
    start_time timestamp,
    user_id int,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    length numeric,
    location varchar,
    user_agent varchar
)
"""
USERS_CREATION = """
CREATE TABLE users (
    user_id int PRIMARY KEY,
    first_name varchar NOT NULL,
    last_name varchar,
    gender varchar,
    level varchar NOT NULL
)
"""
SONGS_CREATION = """
CREATE TABLE songs (
    song_id varchar PRIMARY KEY,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration numeric
)
"""
ARTISTS_CREATION = """
CREATE TABLE artists (
    artist_id varchar PRIMARY KEY,
    name varchar NOT NULL,
    location varchar,
    latitude real,
    longitude real
)
"""
TIME_CREATION = """
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour int NOT NULL,
    day int NOT NULL,
    week int NOT NULL,
    month int NOT NULL,
    year int NOT NULL,
    weekday int NOT NULL
)
"""

# Tables list
TABLES = ["songplays", "songs", "artists", "users", "time"]
CREATE_TABLES_QUERIES = [ARTISTS_CREATION, SONGS_CREATION, USERS_CREATION,
                         TIME_CREATION, SONGPLAYS_CREATION]

# Insert data queries
SONGPLAYS_INSERT = """
INSERT INTO songplays (
    start_time, user_id, song_id, artist_id, session_id, length, location,
    user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
USERS_INSERT = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    gender = EXCLUDED.gender,
    level = EXCLUDED.level
"""
SONGS_INSERT = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO UPDATE SET
    title = EXCLUDED.title,
    artist_id = EXCLUDED.artist_id,
    year = EXCLUDED.year,
    duration = EXCLUDED.duration
"""
ARTISTS_INSERT = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET
    name = EXCLUDED.name,
    location = EXCLUDED.location,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude
"""
TIME_INSERT = """
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (start_time) DO NOTHING
"""

# Song select query
SONG_SELECT = """
SELECT s.song_id, s.artist_id FROM songs s, artists a
WHERE s.artist_id = a.artist_id
    AND s.title = %s AND a.name = %s AND COALESCE(%s, 0) <= s.duration
"""


# Psycopg2 wrapper function
def connect(database="studentdb", autocommit=False):
    """
    Connect to DB.
    Wrapper around try/except block for connection to DB and retrieving a
    cursor to execute queries.
    Args:
        - database: string with database name to connect to.
            Defaults to studentdb.
        - autocommit: boolean to toggle session autocommit. Defaults to True.
    Returns:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    try:
        # Open connection
        connection = psycopg2.connect(f"host=127.0.0.1 dbname={database} \
            user=student password=student")
        connection.set_session(autocommit=True)
        # Get cursor
        cursor = connection.cursor()
    except psycopg2.Error as e:
        print("ERROR: Could not OPEN connection or GET cursor to Postgres DB")
        print(e)
        cursor, connection = None, None
    return cursor, connection


def disconnect(cursor, connection):
    """
    Disconnect from DB.
    Wrapper around try/except block for disconnecting from the DB.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    """
    try:
        cursor.close()
        connection.close()
    except psycopg2.Error as e:
        print("ERROR: Issue disconnecting from DB")
        print(e)


def execute(query, cursor, data=None):
    """
    Execute function.
    Wrapper around try/except block for executing SQL queries with an open
    connection to a DB and a cursor.
    Args:
        - query: string with query to be executed.
        - cursor: cursor object to connected DB.
    Returns:
        - _: boolean with success/fail.
    """
    try:
        if data is None:
            cursor.execute(query)
        else:
            cursor.execute(query, data)
    except psycopg2.Error as e:
        print(f"ERROR: Issue executing query:\n{query}\n")
        print(e)
        return False
    except UnicodeEncodeError:
        # Error thrown with song titles/artist names that are in UTF8
        # as DB (and the DB templates) in ASCII, thus unable to create UTF8 DB.
        return False
    return True


def fetch(cursor):
    """
    Fetch results.
    Function which fetches resutls and returns them.
    Args:
        - cursor: cursor object to connected DB.
    Returns:
        - result: result for fetched query, or None if error.
    """
    try:
        result = cursor.fetchall()
    except psycopg2.Error as e:
        print("ERROR: Issue fetching results.")
        print(e)
        result = None
    return result


def drop(table, cursor):
    """
    Drop table.
    Wrapper around try/except block for dropping specified table with an open
    connection to a DB and a cursor.
    Args:
        - table: string with table to drop.
        - cursor: cursor object to connected DB.
    """
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    except psycopg2.Error as e:
        print(f"ERROR: Issue dropping table {table}.")
        print(e)
