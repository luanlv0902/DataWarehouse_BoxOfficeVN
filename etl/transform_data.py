import os
import re
import json
import glob
import logging
import pandas as pd
from datetime import datetime, date
import mysql.connector

# ==============================================================================
# [1] LOAD CẤU HÌNH
# ==============================================================================
try:
    config_path = 'config/db_config.json'
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    mysql_base = config['mysql']

    # DB Staging
    DB_STAGING_CONFIG = mysql_base.copy()
    DB_STAGING_CONFIG['database'] = config['databases']['staging']

    DB_CONTROL_CONFIG = mysql_base.copy()
    DB_CONTROL_CONFIG["database"] = config["databases"]["control"]

    print("Đã load cấu hình transform thành công")

except Exception as e:
    print(f"Lỗi đọc config.json: {e}")
    exit()

# ==============================================================================
# [2] THIẾT LẬP LOGGING
# ==============================================================================
os.makedirs("logs/transform", exist_ok=True)
log_file = f"logs/transform/transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)
logging.getLogger().addHandler(logging.StreamHandler())

# ==============================================================================
# [3] HÀM CHUẨN HÓA DỮ LIỆU
# ==============================================================================

def normalize_revenue(value):
    """Chuẩn hóa doanh thu: '10.204.018.035' → 10204018035"""
    if pd.isna(value):
        return 0
    return int(str(value).replace(".", "").replace(",", ""))


def normalize_tickets(value):
    """Chuẩn hóa vé: '144.14' → 14414"""
    if pd.isna(value):
        return 0
    return int(str(value).replace(".", "").replace(",", ""))


def normalize_showtimes(value):
    """
    Chuẩn hóa suất chiếu:
    - '3.0'   → 3     (float dạng X.0)
    - '3'     → 3     (số nguyên)
    - '4.766' → 4766  (dạng phân tách hàng nghìn)
    """
    s = str(value).strip()

    # TH1: X.0 → lấy phần nguyên
    if re.match(r'^\d+\.0$', s):
        return int(float(s))

    # TH2: số nguyên
    if s.isdigit():
        return int(s)

    # TH3: phân tách nghìn: 1.234 , 12.345 , 4.766
    if re.match(r'^\d{1,3}(\.\d{3})+$', s):
        return int(s.replace(".", ""))

    # TH4: không hợp lệ
    return 0

# ==============================================================================
# [4] LẤY FILE RAW MỚI NHẤT
# ==============================================================================

def get_latest_raw_file():
    files = glob.glob("data/raw/boxoffice_*.csv")
    if not files:
        return None
    return max(files, key=os.path.getctime)

# ==============================================================================
# [5] HÀM CHÍNH: TRANSFORM
# ==============================================================================

def transform_latest():
    logging.info("BẮT ĐẦU TRANSFORM DỮ LIỆU")

    latest_file = get_latest_raw_file()
    if not latest_file:
        logging.error("Không tìm thấy file RAW nào trong data/raw")
        return
    
    logging.info(f"Đang transform file RAW: {latest_file}")

    # Đọc dữ liệu RAW
    df = pd.read_csv(latest_file)

    # Chuẩn hóa dữ liệu
    df["revenue_clean"] = df["Doanh thu"].apply(normalize_revenue)
    df["tickets_clean"] = df["Vé"].apply(normalize_tickets)
    df["showtimes_clean"] = df["Suất chiếu"].apply(normalize_showtimes)

    # Trích ngày
    try:
        fname = os.path.basename(latest_file)
        date_part = fname.split("_")[1].split(".")[0]
        scraped_date = datetime.strptime(date_part, "%d%m%Y").strftime("%Y-%m-%d")
    except:
        scraped_date = datetime.now().strftime("%Y-%m-%d")

    df["scraped_date"] = scraped_date

    # Tạo thư mục lưu file transform
    os.makedirs("data/transform", exist_ok=True)

    # Tạo tên file output
    today_str = date.today().strftime("%d%m%Y")
    out_path = f"data/transform/boxoffice_transform_{today_str}.csv"

    # Lưu file transform
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    logging.info(f"ĐÃ TẠO FILE TRANSFORM: {out_path}")

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":

    transform_latest()

    # Ghi log vào control DB
    try:
        conn = mysql.connector.connect(**DB_CONTROL_CONFIG)
        cursor = conn.cursor()

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

        # Đọc file log
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split(" - ", 2)
                if len(parts) == 3:
                    log_time_str, level, msg = parts
                    log_time = log_time_str.split(",")[0]

                    cursor.execute("""
                        INSERT INTO etl_log (log_time, log_level, message, source_file)
                        VALUES (%s, %s, %s, %s)
                    """, (log_time, level, msg, os.path.basename(log_file)))

        conn.commit()
        cursor.close()
        conn.close()

        print("Đã ghi log vào Control DB.")

    except Exception as e:
        print(f"Lỗi ghi log DB Control: {e}")