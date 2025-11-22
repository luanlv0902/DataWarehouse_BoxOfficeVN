# etl/load_datamart.py
import os
import sys
import logging
import mysql.connector
from datetime import datetime

# Thiết lập project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db
from etl.aggregate_data import aggregate_for_datamart

# --- Lấy thư mục log từ config DB Control ---
log_dir = get_etl_config_from_db("datamart_log_path") or "logs/datamart"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"load_datamart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Logger setup
logger = logging.getLogger("load_datamart")
logger.setLevel(logging.INFO)
logger.handlers = []  # Xóa handler cũ

file_handler = logging.FileHandler(log_file, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(logging.StreamHandler())  # Console

def load_to_datamart():
    logger.info("Start load_to_datamart")
    
    # --- Lấy dữ liệu aggregate từ warehouse ---
    aggregate_path = get_etl_config_from_db("aggregate_data_path") or "data/aggregate"
    daily_df, top_df = aggregate_for_datamart()
    
    if daily_df is None or top_df is None:
        logger.warning("No aggregate data to load")
        return

    dm_cfg = get_db_config("datamart")
    conn = mysql.connector.connect(**dm_cfg)
    cur = conn.cursor()

    # --- Load dm_daily_revenue ---
    data_daily = daily_df[['movie_name', 'full_date', 'revenue_vnd', 'tickets_sold', 'showtimes']].values.tolist()
    if data_daily:
        cur.executemany("""
            INSERT INTO dm_daily_revenue (movie_name, full_date, revenue_vnd, tickets_sold, showtimes)
            VALUES (%s, %s, %s, %s, %s)
        """, data_daily)
        conn.commit()
        logger.info(f"Inserted {len(data_daily)} rows into dm_daily_revenue")
    else:
        logger.warning("No rows to insert into dm_daily_revenue")

    # --- Load dm_top_movies ---
    data_top = top_df[['movie_name', 'revenue_vnd', 'tickets_sold', 'showtimes', 'ranking']].values.tolist()
    if data_top:
        cur.executemany("""
            INSERT INTO dm_top_movies (movie_name, total_revenue, total_tickets, total_showtimes, ranking)
            VALUES (%s, %s, %s, %s, %s)
        """, data_top)
        conn.commit()
        logger.info(f"Inserted {len(data_top)} rows into dm_top_movies")
    else:
        logger.warning("No rows to insert into dm_top_movies")

    cur.close()
    conn.close()

    # --- Flush log và push vào db_control ---
    try:
        for handler in logger.handlers:
            if isinstance(handler, logging.FileHandler):
                handler.flush()
        if os.path.exists(log_file):
            push_log_file_to_db(log_file, get_db_config("control"))
            logger.info("Pushed log to db_control successfully")
        else:
            logger.warning(f"Log file not found for push: {log_file}")
    except Exception:
        logger.exception("Push log to db_control failed")

if __name__ == "__main__":
    load_to_datamart()