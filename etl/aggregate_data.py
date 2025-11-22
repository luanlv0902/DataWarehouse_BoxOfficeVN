# etl/aggregate_data.py
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

# Logging
os.makedirs("logs/aggregate", exist_ok=True)
log_file = f"logs/aggregate/aggregate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def aggregate_for_datamart():
    logging.info("Start aggregate_for_datamart")
    
    # Connect to warehouse
    cfg = get_db_config("warehouse")
    conn = mysql.connector.connect(**cfg)
    
    # Load fact + dim data
    sql = """
    SELECT 
        f.revenue_id,
        m.movie_name,
        d.full_date,
        f.revenue_vnd,
        f.tickets_sold,
        f.showtimes
    FROM fact_revenue f
    JOIN dim_movie m ON f.movie_key = m.movie_key
    JOIN dim_date d ON f.date_key = d.date_key
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    
    if df.empty:
        logging.warning("No data found in fact_revenue")
        return None, None
    
    # 1️⃣ Aggregate daily revenue
    daily_df = df.groupby(['movie_name', 'full_date'], as_index=False).agg({
        'revenue_vnd': 'sum',
        'tickets_sold': 'sum',
        'showtimes': 'sum'
    })
    
    # 2️⃣ Aggregate top movies (total revenue)
    top_df = df.groupby(['movie_name'], as_index=False).agg({
        'revenue_vnd': 'sum',
        'tickets_sold': 'sum',
        'showtimes': 'sum'
    }).sort_values(by='revenue_vnd', ascending=False).reset_index(drop=True)
    top_df['ranking'] = top_df.index + 1
    
    # Optional: save CSV backup
    os.makedirs("data/aggregate", exist_ok=True)
    today_str = datetime.today().strftime("%d%m%Y")
    daily_path = f"data/aggregate/dm_daily_revenue_{today_str}.csv"
    top_path = f"data/aggregate/dm_top_movies_{today_str}.csv"
    daily_df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    top_df.to_csv(top_path, index=False, encoding="utf-8-sig")
    logging.info(f"Daily revenue CSV saved: {daily_path}")
    logging.info(f"Top movies CSV saved: {top_path}")
    
    # Push logs to control DB
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Push log failed")

    return daily_df, top_df

if __name__ == "__main__":
    aggregate_for_datamart()