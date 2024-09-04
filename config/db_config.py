# config/db_config.py
import psycopg2
from psycopg2 import sql

def get_connection():
    """
    Tries to connect to PostgreSQL database.

    Returns
    -------
    connection : psycopg2.extensions.connection
        The connection object if the connection was successful, None otherwise.
    """
    try:
        # Connect to PostgreSQL database
        connection = psycopg2.connect(
            # Name of the database to connect to
            dbname="covid_monitoring",
            # Username to use for the connection
            user="postgres",
            # Password to use for the connection
            password="123",
            # Hostname of the PostgreSQL server
            host="localhost",
            # Port number to use for the connection
            port="5432"
        )
        print("Connection to the PostgreSQL database was successful.")
        return connection
    except Exception as e:
        # If there were any errors, print them to the console
        print(f"Error connecting to the database: {e}")
    return None
