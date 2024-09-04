import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import logging

logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Function to load data into PostgreSQL
def load_to_postgresql(df, table_name, conn_str):
    try:
        # Create connection to PostgreSQL using SQLAlchemy
        engine = create_engine(conn_str)
        
        # Convert data types for compatibility with PostgreSQL
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')  # Convert datetime to string
        
        if 'population' in df.columns:
            df['population'] = df['population'].astype(int)  # Convert to integer type
        
        # Load DataFrame into the database
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Data loaded successfully into table {table_name}.")
    except Exception as e:
        logging.error(f"Error loading data into table {table_name}: {e}")

# Function to transform COVID-19 cases data
def transform_covid_cases(cases_df):
    try:
        cases_df = cases_df.rename(columns=lambda x: x.strip())
        cases_df = cases_df.drop(columns=['Lat', 'Long'], errors='ignore')
        cases_df = cases_df.melt(id_vars=['Province/State', 'Country/Region'],
                                 var_name='date', value_name='confirmed_cases')
        cases_df = cases_df.groupby(['Country/Region', 'date']).sum().reset_index()
        cases_df.rename(columns={'Country/Region': 'country'}, inplace=True)
        cases_df['date'] = pd.to_datetime(cases_df['date'])
        logging.info("COVID-19 cases data transformed successfully.")

        print(cases_df)
        return cases_df
    except Exception as e:
        logging.error(f"Error transforming COVID-19 cases data: {e}")
        return None

# Function to transform vaccination data
def transform_vaccination_data(vaccination_df):
    try:
        vaccination_df = vaccination_df.rename(columns=lambda x: x.strip())
        required_columns = ['location', 'date', 'total_vaccinations', 'people_vaccinated', 
                            'people_fully_vaccinated', 'daily_vaccinations']
        missing_columns = [col for col in required_columns if col not in vaccination_df.columns]
        if missing_columns:
            raise ValueError(f"The following columns are missing: {missing_columns}")
        vaccination_df = vaccination_df[required_columns]
        vaccination_df.rename(columns={'location': 'country'}, inplace=True)
        vaccination_df['date'] = pd.to_datetime(vaccination_df['date'])
        logging.info("Vaccination data transformed successfully.")

        print(vaccination_df)
        return vaccination_df
    except Exception as e:
        logging.error(f"Error transforming vaccination data: {e}")
        return None

# Function to transform country population data
def transform_country_population(population_df):
    try:
        population_df = population_df.rename(columns=lambda x: x.strip())
        required_columns = ['country', 'population']
        missing_columns = [col for col in required_columns if col not in population_df.columns]
        if missing_columns:
            raise ValueError(f"The following columns are missing: {missing_columns}")
        population_df = population_df[required_columns]
        population_df.rename(columns={'country': 'country'}, inplace=True)
        population_df['population'] = population_df['population'].astype(int)
        logging.info("Country population data transformed successfully.")
        print(population_df)
        return population_df
    except Exception as e:
        logging.error(f"Error transforming country population data: {e}")
        return None

