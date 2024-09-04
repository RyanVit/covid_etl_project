import pandas as pd
import logging

# Configuração do logging
logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def extract_csv(file_path):
    """
    Generic function to extract data from a CSV file.
    """
    try:
        data_df = pd.read_csv(file_path)
        num_rows = len(data_df)
        logging.info(f"Data successfully extracted from file: {file_path}. Total rows extracted: {num_rows}.")
        return data_df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except pd.errors.EmptyDataError:
        logging.error(f"File empty: {file_path}")
        return None
    except pd.errors.ParserError:
        logging.error(f"Error parsing file: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error extracting data from file {file_path}: {e}")
        return None

def extract_covid_cases():
    """
    Function to extract data from COVID-19 cases.
    """
    df = extract_csv('./data/covid_cases.csv')
    if df is not None:
        print(f"Data successfully extracted from file: {len(df)}.")
    return df

def extract_vaccination_data():
    """
    Function to extract data from vaccination.
    """
    df = extract_csv('./data/vaccination_data.csv')
    if df is not None:
        print(f"Data successfully extracted from file: {len(df)}.")
    return df

def extract_country_population():
    """
    Function to extract data from country population.
    """
    df = extract_csv('./data/country_population.csv')
    if df is not None:
        print(f"Data successfully extracted from file: {len(df)}.")
    return df

