# main.py
import sys
import os
import logging
from datetime import datetime

# --- Thiết lập project root ---
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

# Utils
from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db

# ETL Steps
from etl.extract_data import scrape_to_csv
from etl.load_staging import run_staging_load
from etl.transform_data import transform_latest_to_csv
from etl.load_datawarehouse import run_warehouse_load
from etl.aggregate_data import aggregate_for_datamart
from etl.load_datamart import load_to_datamart

# --- Logger chung cho main ---
log_dir = get_etl_config_from_db("etl_log_path") or "logs/main"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logger = logging.getLogger("ETL_Pipeline")
logger.setLevel(logging.INFO)
logger.handlers = []

file_handler = logging.FileHandler(log_file, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())

def run_full_etl():
    logger.info("Starting full ETL pipeline")

    try:
        # 1. Extract
        logger.info("Step 1: Extract data")
        raw_file = scrape_to_csv()
        logger.info(f"Extract finished: {raw_file}")

        # 2️. Load staging
        logger.info("Step 2: Load staging")
        run_staging_load()
        logger.info("Staging load finished")

        # 3️. Transform
        logger.info("Step 3: Transform data")
        cleaned_df = transform_latest_to_csv()
        logger.info(f"Transform finished, {len(cleaned_df) if cleaned_df is not None else 0} rows processed")

        # 4️. Load warehouse
        logger.info("Step 4: Load warehouse")
        run_warehouse_load()
        logger.info("Warehouse load finished")

        # 5️. Aggregate data
        logger.info("Step 5: Aggregate data")
        daily_df, top_df = aggregate_for_datamart()
        logger.info(f"Aggregate finished: {len(daily_df) if daily_df is not None else 0} daily rows, "
                    f"{len(top_df) if top_df is not None else 0} top movie rows")

        # 6️. Load datamart
        logger.info("Step 6: Load datamart")
        load_to_datamart()
        logger.info("Datamart load finished")

    except Exception as e:
        logger.exception(f"ETL pipeline failed: {e}")

    finally:
        # Push log vào db_control
        try:
            for handler in logger.handlers:
                if isinstance(handler, logging.FileHandler):
                    handler.flush()
            if os.path.exists(log_file):
                push_log_file_to_db(log_file, get_db_config("control"))
                logger.info("Pushed main ETL log to db_control successfully")
            else:
                logger.warning(f"Log file not found for push: {log_file}")
        except Exception:
            logger.exception("Push main ETL log to db_control failed")

    logger.info("=== ETL pipeline finished ===")


if __name__ == "__main__":
    run_full_etl()