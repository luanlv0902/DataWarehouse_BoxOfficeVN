# etl/load_staging.py
import sys
import os
import glob
import logging
import pandas as pd
import mysql.connector
from datetime import datetime
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db

# Logging
log_dir = "logs/staging"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/load_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def get_latest_raw_file():
    # Lấy thư mục raw từ DB
    raw_dir = get_etl_config_from_db("raw_data_path") or "data/raw"
    files = glob.glob(os.path.join(raw_dir, "boxoffice_*.csv"))
    return max(files, key=os.path.getctime) if files else None

def run_staging_load():
    logging.info("Start load_staging")
    raw = get_latest_raw_file()
    if not raw:
        logging.error("No raw CSV found")
        return

    logging.info(f"Loading raw file: {raw}")
    df = pd.read_csv(raw, encoding="utf-8-sig")

    # extract date from filename if present
    try:
        base = os.path.basename(raw)
        date_part = base.split("_")[1].split(".")[0]
        scraped_date = datetime.strptime(date_part, "%d%m%Y").date()
    except Exception:
        scraped_date = datetime.today().date()

    cfg = get_db_config("staging")
    conn = mysql.connector.connect(**cfg)
    cur = conn.cursor()

    # TRUNCATE then insert
    cur.execute("TRUNCATE TABLE stg_boxoffice_raw")
    insert_sql = """
        INSERT INTO stg_boxoffice_raw (film_name, revenue_raw, tickets_raw, showtimes_raw, scraped_date)
        VALUES (%s,%s,%s,%s,%s)
    """
    data = [(r.get("Tên phim"), r.get("Doanh thu"), r.get("Vé"), r.get("Suất chiếu"), scraped_date)
            for _, r in df.iterrows()]
    if data:
        cur.executemany(insert_sql, data)
        conn.commit()
        logging.info(f"Inserted {cur.rowcount} rows into stg_boxoffice_raw")

    cur.close()
    conn.close()

    # push logs vào db_control
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Failed to push logs to DB control")

if __name__ == "__main__":
    run_staging_load()