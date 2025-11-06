import pandas as pd
import os
import time
from sqlalchemy import create_engine
import sqlite3
import logging

# ---- Setup base directory ----
BASE_DIR = r"C:\Users\ap505\Documents\Vendor Performance Data Analytics project"
os.chdir(BASE_DIR)
os.makedirs("logs", exist_ok=True)

# ---- Logging setup ----
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filemode="a",
    force=True  # Ensures custom config overrides any existing one
)

# ---- Database setup ----
DB_PATH = os.path.join(BASE_DIR, "inventory.db")
engine = create_engine(f"sqlite:///{DB_PATH}")

# ---- Function to ingest data ----
def ingest_db(df, table_name, engine):
    """This function ingests the dataframe into the database table"""
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Table '{table_name}' created successfully.")

# ---- Function to load and ingest raw data ----
def load_raw_data():
    """This function loads the CSVs as dataframe and ingests into db"""
    start = time.time()
    data_dir = os.path.join(BASE_DIR, 'data')

    if not os.path.exists(data_dir):
        logging.error(f"Data directory not found: {data_dir}")
        print(f"Error: Data directory not found: {data_dir}")
        return

    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            csv_path = os.path.join(data_dir, file)
            df = pd.read_csv(csv_path)
            logging.info(f"Ingesting {file} into db")
            ingest_db(df, file[:-4], engine)

    end = time.time()
    total_time = (end - start) / 60
    logging.info("-----------Ingestion Complete-----------")
    logging.info(f"Total time taken: {total_time:.2f} minutes")
    print("Ingestion complete! Check logs/ingestion_db.log")



# ---- Main execution ----
if __name__ == '__main__':
    load_raw_data()