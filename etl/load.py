# etl/load.py
import pandas as pd
import numpy as np
from config.db_config import get_connection
import logging

logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_data_to_db(df, table_name):
    conn = get_connection()
    if conn is None:
        logging.error("Connection to the database failed.")
        return
    cursor = conn.cursor()
    try:
        # Convert the dataframe to a list of tuples
        records = df.to_records(index=False)
        records_list = [[
            pd.to_datetime(x).isoformat() if isinstance(x, pd.Timestamp) else x
            if not isinstance(x, np.datetime64) and not isinstance(x, np.int64) else str(x)
            for x in row
        ] for row in records]
        
        
        # Generate dynamic SQL query
        cols = ",".join(list(df.columns))
        placeholders = ",".join(["%s"] * len(df.columns))
        query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
        
        # Execute insertion in batch
        cursor.executemany(query, records_list)
        conn.commit()
        logging.info(f"Data loaded successfully into table {table_name}.")
        logging.info(f"----------------------------------------------------")
    except Exception as e:
        conn.rollback()
        if "population" in str(e) and "country" in str(e):
            logging.error(f"Error loading data into table {table_name}: The column 'country_name' does not exist in the table {table_name}. The values must be 'country' and 'population'.")
        else:
            logging.error(f"Error loading data into table {table_name}: {e}")
    finally:
        cursor.close()
        conn.close()

