# intialize_database.py

import sqlite3
import logging

def connect_to_db(database_file_name):
    """ Returns connection to the database

    Arguments:
        database_file_name(str) - pathname or filename to the database file
    """

    connection = sqlite3.connect(database_file_name)
    logging.info(f"Connected to {database_file_name} database")
    return connection

def create_customer_table(connection, table_name ):
    """ creates customer table in sqlite database using the connection

    Arguments:
        connection(sqlite3.connection) : connection to sqlite database
        table_name(str) : Table name
    """
    cursor = connection.cursor()
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS {table_name}
                  (customer_id INTEGER PRIMARY KEY,
                  first_name TEXT NOT NULL,
                  last_name TEXT NOT NULL,
                  email TEXT NOT NULL UNIQUE,
                  gender TEXT NOT NULL,
                  ip_address TEXT)''')
    logging.info(f"Created {table_name} table if it doesn't exist")

    connection.commit()
