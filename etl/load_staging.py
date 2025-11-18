import os
import glob
import json
import logging
import pandas as pd
import mysql.connector
from datetime import datetime

# ==============================================================================
# [2.1] LOAD CẤU HÌNH TỪ FILE JSON
# ==============================================================================
try:
    # Nếu file chạy từ gốc dự án, đường dẫn là 'config.json'
    config_path = 'config/db_config.json' 
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    mysql_base = config['mysql']

    # Cấu hình DB Staging
    DB_STAGING_CONFIG = mysql_base.copy()
    DB_STAGING_CONFIG['database'] = config['databases']['staging']

    # Cấu hình DB Control (để ghi log)
    DB_CONTROL_CONFIG = mysql_base.copy()
    DB_CONTROL_CONFIG['database'] = config['databases']['control']

    print("Đã load cấu hình từ config.json")

except Exception as e:
    print(f"Lỗi đọc file config.json: {e}")
    exit()

# ==============================================================================
# [2.2] THIẾT LẬP LOGGING
# ==============================================================================
os.makedirs("logs/staging", exist_ok=True)
log_file = f"logs/staging/load_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)
logging.getLogger().addHandler(logging.StreamHandler())

# ==============================================================================
# [HÀM HỖ TRỢ] TÌM FILE CSV RAW MỚI NHẤT
# ==============================================================================
def get_latest_raw_file():
    """Tìm file CSV trong thư mục data/raw"""
    raw_dir = "data/raw"
    files = glob.glob(f'{raw_dir}/boxoffice_*.csv')
    if not files:
        return None
    return max(files, key=os.path.getctime)

# ==============================================================================
# [2.3] HÀM CHÍNH: LOAD DATA VÀO STAGING (RAW TABLE)
# ==============================================================================
def load_raw_to_staging():
    logging.info("------- BẮT ĐẦU LOAD STAGING (RAW) -------")

    # [2.4] Tìm file CSV Raw
    latest_file = get_latest_raw_file()
    if not latest_file:
        # [2.5.1] Ghi log lỗi và Dừng
        logging.error("Không tìm thấy file CSV nào trong data/raw")
        return

    logging.info(f"Đang xử lý file: {latest_file}")

    try:
        # [2.5.2] Đọc file CSV
        df = pd.read_csv(latest_file)
        
        # Lấy ngày cào từ tên file (boxoffice_18112025.csv) để làm scraped_date
        try:
            file_name = os.path.basename(latest_file)
            date_part = file_name.split('_')[1].split('.')[0]
            scraped_date = datetime.strptime(date_part, "%d%m%Y").strftime("%Y-%m-%d")
        except:
            scraped_date = datetime.now().strftime("%Y-%m-%d")

        # [2.6] Kết nối DB Staging
        conn = mysql.connector.connect(**DB_STAGING_CONFIG)
        cursor = conn.cursor()

        # [2.7] Xóa dữ liệu cũ (Truncate) để nạp mới
        # Lưu ý: Tùy chiến lược, có thể bạn muốn giữ lịch sử thì bỏ dòng này
        cursor.execute("TRUNCATE TABLE stg_boxoffice_raw")

        # [2.8] Chuẩn bị dữ liệu Insert
        sql = """
            INSERT INTO stg_boxoffice_raw 
            (film_name, revenue_raw, tickets_raw, showtimes_raw, scraped_date, source) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        data_to_insert = []
        # Map cột trong CSV (Tiếng Việt) sang cột DB (Tiếng Anh)
        # CSV: "Tên phim", "Doanh thu", "Vé", "Suất chiếu"
        for _, row in df.iterrows():
            data_to_insert.append((
                row['Tên phim'],
                row['Doanh thu'],
                row['Vé'],
                row['Suất chiếu'],
                scraped_date,
                "BoxOfficeVietnam"
            ))

        cursor.executemany(sql, data_to_insert)
        # [2.9] Commit & Đóng kết nối Staging
        conn.commit()
        
        logging.info(f"Đã nạp thành công {cursor.rowcount} dòng vào stg_boxoffice_raw")
        
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Lỗi Load Staging: {e}", exc_info=True)

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":
    
    # [2.3] Chạy quy trình Load
    load_raw_to_staging()

    # [2.10] Đồng bộ Log vào Database Control
    try:
        print("Dang nap log vao DB Control...")
        conn = mysql.connector.connect(**DB_CONTROL_CONFIG)
        cursor = conn.cursor()

        # Tạo bảng log nếu chưa có
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                log_time DATETIME,
                log_level VARCHAR(10),
                message TEXT,
                source_file VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # [2.11] Mở file Log -> Đọc từng dòng
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                 # [2.12] Tách dòng log & Insert vào DB
                parts = line.strip().split(" - ", 2)
                if len(parts) == 3:
                    log_time_str, log_level, message = parts
                    # Xử lý format thời gian log (bỏ phần milliseconds)
                    log_time = log_time_str.split(',')[0]
                    
                    cursor.execute("""
                        INSERT INTO etl_log (log_time, log_level, message, source_file)
                        VALUES (%s, %s, %s, %s)
      
                    """, (log_time, log_level, message, os.path.basename(log_file)))
 
        # [2.13] Commit & Kết thúc chương trình

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Hoan tat quy trinh.")
        print("XONG.")

    except Exception as e:
        print(f"Lỗi nạp log vào DB: {e}")