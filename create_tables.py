"""
Create Tables script.
Module which creates the project's tables. The use of this module assumes an
available connection to a PostgreSQL database with the given credentials.
"""
import psycopg2
import sql_queries as sql


def create_database(cursor, connection):
    """
    Create sparkify DB.
    Function which takes an opened connection and cursor to studentdb, creates
    a new database sparkifydb (if not exists) and changes connection to it.
    Args:
        - cursor: cursor object to connected DB.
        - connection: connection object for currently connected DB.
    Returns:
        - cursor: cursor object to connected sparkifydb.
        - connection: connection object for currently connected sparkifydb.
    """
    try:
        sql.execute("DROP DATABASE IF EXISTS sparkifydb", cursor)
        # Create UTF8 DB template in DBMG
        sql.execute("UPDATE pg_database SET datistemplate = FALSE \
            WHERE datname = 'template1'", cursor)
        sql.execute("DROP DATABASE template1", cursor)
        sql.execute("CREATE DATABASE template1 WITH TEMPLATE = template0 \
            ENCODING = 'UTF8';", cursor)
        sql.execute("UPDATE pg_database SET datistemplate = TRUE \
            WHERE datname = 'template1'", cursor)
        # Create new UTF encoded sparkifydb
        sql.execute("CREATE DATABASE sparkifydb ENCODING='UTF8'", cursor)
    except psycopg2.Error as e:
        print("ERROR: Issue creating sparkifydb DB.")
        print(e)
    # Disconnect current DB
    sql.disconnect(cursor, connection)
    # Connect to new DB
    return sql.connect(database="sparkifydb")


def drop_tables(cursor):
    """
    Drop project tables.
    Function which drops projects tables.
    Args:
        - cursor: cursor object to connected sparkifydb.
    """
    for table in sql.TABLES:
        sql.drop(table, cursor)


def create_tables(cursor):
    """
    Create project tables.
    Function which creates projects tables.
    Args:
        - cursor: cursor object to connected sparkifydb.
    """
    for table in sql.CREATE_TABLES_QUERIES:
        sql.execute(table, cursor)


def main():
    """
    Main function.
    Main function to execute when module is called through command line.
    Creates a connection to DB studentdb, creates (if not exists) the sparkify
    DB and changes conenction to it, drops the tables (if exist) and creates
    them again.
    """
    # Get connections
    cursor, connection = sql.connect()
    if not cursor:  # End execution if error
        return
    # Create sparkify database and change connection
    cursor, connection = create_database(cursor, connection)
    if not cursor:  # End execution if error
        return
    # Drop tables if exist
    drop_tables(cursor)
    # Create dimension tables
    create_tables(cursor)
    # Disconnect
    sql.disconnect(cursor, connection)


if __name__ == "__main__":
    main()
