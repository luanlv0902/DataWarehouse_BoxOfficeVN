# etl/transform_data.py
import os
import sys
import re
import logging
import pandas as pd
import mysql.connector
from datetime import datetime, date
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db

# Logging
log_dir = "logs/transform"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def normalize_revenue(v):
    if pd.isna(v):
        return 0
    s = str(v).strip().replace(",", "").replace(".", "")
    return int(s) if s.isdigit() else 0

def normalize_tickets(v):
    if pd.isna(v):
        return 0
    s = str(v).strip().replace(",", "").replace(".", "")
    return int(s) if s.isdigit() else 0

def normalize_showtimes(v):
    if pd.isna(v):
        return 0
    s = str(v).strip()
    if re.match(r'^\d+\.0$', s):
        return int(float(s))
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        return int(s.replace(".", ""))
    if s.isdigit():
        return int(s)
    s2 = s.replace(",", ".")
    try:
        return int(float(s2))
    except:
        return 0

def transform_latest_to_csv():
    logging.info("Start transform")
    cfg = get_db_config("staging")
    conn = mysql.connector.connect(**cfg)
    df = pd.read_sql("SELECT * FROM stg_boxoffice_raw ORDER BY id DESC", conn)
    conn.close()

    if df.empty:
        logging.warning("No data in staging")
        return None

    df["film_name"] = df["film_name"].astype(str).str.strip()
    df["revenue_clean"] = df["revenue_raw"].apply(normalize_revenue)
    df["tickets_clean"] = df["tickets_raw"].apply(normalize_tickets)
    df["showtimes_clean"] = df["showtimes_raw"].apply(normalize_showtimes)
    df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date

    # --- Lấy thư mục lưu trữ cleaned data từ DB ---
    cleaned_dir = get_etl_config_from_db("cleaned_data_path") or "data/cleaned"
    os.makedirs(cleaned_dir, exist_ok=True)
    today_str = date.today().strftime("%d%m%Y")
    out_path = os.path.join(cleaned_dir, f"boxoffice_cleaned_{today_str}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logging.info(f"Wrote cleaned CSV: {out_path}")

    # push logs vào db_control
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Push log failed")

    return df

if __name__ == "__main__":
    transform_latest_to_csv()