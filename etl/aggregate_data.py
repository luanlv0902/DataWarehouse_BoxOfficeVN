# etl/aggregate_data.py
import os
import sys
import logging
import pandas as pd
import mysql.connector
from datetime import datetime
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db

# 1. Khởi tạo logging động
log_dir = "logs/aggregate"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/aggregate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

def aggregate_for_datamart():
    logging.info("Start aggregate_for_datamart")
    
    # 2. Kết nối data warehouse
    cfg = get_db_config("warehouse")
    conn = mysql.connector.connect(**cfg)
    # 3. Truy vấn dữ liệu từ các bảng dim + fact
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
    #4. Đưa kết quả vào dataFrame Pandas
    df = pd.read_sql(sql, conn)
    conn.close()
    
    if df.empty:
        logging.warning("No data found in fact_revenue")
        return None, None
    
    # 5. Tổng hợp dữ liệu daily revenue
    # Aggregate daily revenue
    daily_df = df.groupby(['movie_name', 'full_date'], as_index=False).agg({
        'revenue_vnd': 'sum',
        'tickets_sold': 'sum',
        'showtimes': 'sum'
    })
    
    # 6. Tổng hợp danh sách top movies
    # Aggregate top movies
    top_df = df.groupby(['movie_name'], as_index=False).agg({
        'revenue_vnd': 'sum',
        'tickets_sold': 'sum',
        'showtimes': 'sum'
    }).sort_values(by='revenue_vnd', ascending=False).reset_index(drop=True) # 7. Sắp xếp doanh thu
    top_df['ranking'] = top_df.index + 1
    
    # 8. Lấy đường dẫn aggregate động từ db_control
    aggregate_dir = get_etl_config_from_db("aggregate_data_path") or "data/aggregate"
    os.makedirs(aggregate_dir, exist_ok=True)
    
    # 9. Sinh output CSV cho data mart
    today_str = datetime.today().strftime("%d%m%Y")
    daily_path = os.path.join(aggregate_dir, f"dm_daily_revenue_{today_str}.csv")
    top_path = os.path.join(aggregate_dir, f"dm_top_movies_{today_str}.csv")
    
    daily_df.to_csv(daily_path, index=False, encoding="utf-8-sig")
    top_df.to_csv(top_path, index=False, encoding="utf-8-sig")
    
    logging.info(f"Daily revenue CSV saved: {daily_path}")
    logging.info(f"Top movies CSV saved: {top_path}")
    
    # 10. Đẩy log vào db_control
    try:
        push_log_file_to_db(log_file, get_db_config("control"))
    except Exception:
        logging.exception("Push log failed")
    
    return daily_df, top_df

if __name__ == "__main__":
    aggregate_for_datamart()