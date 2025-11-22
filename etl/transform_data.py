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
from utils.db_connection import get_db_config
from utils.log_to_db import push_log_file_to_db

# logging
os.makedirs("logs/transform", exist_ok=True)
log_file = f"logs/transform/transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def normalize_revenue(v):
    if pd.isna(v):
        return 0
    s = str(v).strip()
    # remove thousand separators (.) and commas
    s = s.replace(",", "").replace(".", "")
    return int(s) if s.isdigit() else 0

def normalize_tickets(v):
    if pd.isna(v):
        return 0
    s = str(v).strip()
    s = s.replace(",", "").replace(".", "")
    return int(s) if s.isdigit() else 0

def normalize_showtimes(v):
    if pd.isna(v):
        return 0
    s = str(v).strip()
    # X.0 -> integer part
    if re.match(r'^\d+\.0$', s):
        return int(float(s))
    # thousand separators 1.234, 12.345
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        return int(s.replace(".", ""))
    # pure digits
    if s.isdigit():
        return int(s)
    # if decimal with comma
    s2 = s.replace(",", ".")
    try:
        f = float(s2)
        return int(f)
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

    os.makedirs("data/cleaned", exist_ok=True)
    today_str = date.today().strftime("%d%m%Y")
    out_path = f"data/cleaned/boxoffice_cleaned_{today_str}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logging.info(f"Wrote cleaned CSV: {out_path}")

    # push logs to control
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Push log failed")

    return df

if __name__ == "__main__":
    transform_latest_to_csv()