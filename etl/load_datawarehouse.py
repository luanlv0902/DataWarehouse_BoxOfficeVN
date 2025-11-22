# etl/load_warehouse.py
import os
import sys
import logging
import pandas as pd
import mysql.connector
from datetime import datetime
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
from utils.db_connection import get_db_config
from utils.log_to_db import push_log_file_to_db

# logging
os.makedirs("logs/warehouse", exist_ok=True)
log_file = f"logs/warehouse/load_warehouse_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def get_latest_cleaned_csv():
    files = sorted([f for f in os.listdir("data/cleaned") if f.startswith("boxoffice_cleaned_")])
    return os.path.join("data/cleaned", files[-1]) if files else None

def run_warehouse_load():
    logging.info("Start load_warehouse")
    csv_file = get_latest_cleaned_csv()
    if not csv_file or not os.path.exists(csv_file):
        logging.error("No cleaned CSV found")
        return

    df = pd.read_csv(csv_file, encoding="utf-8-sig")
    if df.empty:
        logging.warning("Cleaned CSV empty")
        return

    wh_cfg = get_db_config("warehouse")
    conn = mysql.connector.connect(**wh_cfg)
    cur = conn.cursor()

    # Ensure dim_movie exists - insert unique movie_name
    cur.execute("SELECT movie_key, movie_name FROM dim_movie")
    existing = {name: key for key, name in cur.fetchall()}

    movies_to_insert = [name for name in df["film_name"].unique() if name not in existing]
    for m in movies_to_insert:
        cur.execute("INSERT INTO dim_movie (movie_name) VALUES (%s)", (m,))
        conn.commit()
        existing[m] = cur.lastrowid

    logging.info(f"dim_movie updated, total movies now: {len(existing)}")

    # Ensure dim_date entries
    cur.execute("SELECT date_key FROM dim_date")
    existing_dates = set([r[0] for r in cur.fetchall()])

    # prepare fact inserts
    fact_rows = []
    for _, r in df.iterrows():
        # date_key = YYYYMMDD as int
        try:
            dd = pd.to_datetime(r["scraped_date"]).date()
            date_key = int(dd.strftime("%Y%m%d"))
        except:
            continue

        if date_key not in existing_dates:
            cur.execute("INSERT INTO dim_date (date_key, full_date, year, month, day, quarter) VALUES (%s,%s,%s,%s,%s,%s)",
                        (date_key, dd, dd.year, dd.month, dd.day, (dd.month-1)//3+1))
            conn.commit()
            existing_dates.add(date_key)

        movie_key = existing.get(r["film_name"])
        revenue = int(r.get("revenue_clean", 0))
        tickets = int(r.get("tickets_clean", 0))
        showtimes = int(r.get("showtimes_clean", 0))

        fact_rows.append((movie_key, date_key, revenue, tickets, showtimes, datetime.now()))

    if fact_rows:
        cur.executemany("""
            INSERT INTO fact_revenue (movie_key, date_key, revenue_vnd, tickets_sold, showtimes, load_date)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, fact_rows)
        conn.commit()
        logging.info(f"Inserted {cur.rowcount} rows into fact_revenue")

    cur.close()
    conn.close()

    # push logs
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Push log failed")

if __name__ == "__main__":
    run_warehouse_load()