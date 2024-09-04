# main.py
from etl.extract import extract_covid_cases, extract_vaccination_data, extract_country_population
from etl.transform import transform_covid_cases, transform_vaccination_data, transform_country_population
from etl.load import load_data_to_db
import logging

logging.basicConfig(filename='logs/etl.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def run_etl():
    logging.info("Starting ETL process.")

    # Extraction
    cases_df = extract_covid_cases()
    vaccination_df = extract_vaccination_data()
    population_df = extract_country_population()

    if cases_df is None or vaccination_df is None or population_df is None:
        logging.error("Data extraction failed. ETL process terminated.")
        return

    # Transformation
    cases_df = transform_covid_cases(cases_df)
    vaccination_df = transform_vaccination_data(vaccination_df)
    population_df = transform_country_population(population_df)

    if cases_df is None or vaccination_df is None or population_df is None:
        logging.error("Data transformation failed. ETL process terminated.")
        return

    # Load
    load_data_to_db(cases_df, 'covid_cases')
    load_data_to_db(vaccination_df, 'vaccination_data')
    load_data_to_db(population_df, 'country_population')

    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()


