import logging
from extract import extract_all
from transform import transform
from load import create_table, insert_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def run_pipeline():
    logging.info("starting the pipeline")

    try:
        logging.info("Extracting data...")
        raw=extract_all()
        logging.info("Transforming data...")
        transformed=transform(raw)
        logging.info("Loading data...")
        create_table()
        insert_data(transformed)
        logging.info("Pipeline completed successfully")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise

if __name__=="__main__":
    run_pipeline()