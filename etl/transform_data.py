# === CÀI ĐẶT THƯ VIỆN CẦN THIẾT ===
# Nếu bạn chưa cài đặt, hãy chạy lệnh sau trong terminal:
# pip install pandas mysql-connector-python

import os
import pandas as pd
import logging
from datetime import datetime, date
import re # Mặc dù không sử dụng trực tiếp re.sub, nhưng được giữ lại nếu có nhu cầu phức tạp hơn
import mysql.connector

# ==============================================================================
# 1. KHỞI ĐỘNG QUY TRÌNH & CẤU HÌNH LOGGING
# ==============================================================================

# Tạo thư mục log cho quá trình transform nếu chưa tồn tại
os.makedirs("logs/transform", exist_ok=True)

# Định nghĩa tên file log động dựa trên thời gian hiện tại
log_file = f"logs/transform/etl_transform_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Cấu hình logging: ghi vào file và hiển thị trên console
logging.basicConfig(
    filename=log_file,
    level=logging.INFO, # Ghi lại các thông báo INFO, WARNING, ERROR, CRITICAL
    format="%(asctime)s - %(levelname)s - %(message)s", # Định dạng của mỗi dòng log
    encoding="utf-8" # Hỗ trợ tiếng Việt trong log
)
# Thêm console handler để log hiển thị trên màn hình
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

logging.info("BẮT ĐẦU QUY TRÌNH TRANSFORM DỮ LIỆU BOXOFFICEVIETNAM")

# ==============================================================================
# 2. ĐỊNH NGHĨA ĐƯỜNG DẪN
# ==============================================================================

RAW_DATA_DIR = "data/raw" # Thư mục chứa dữ liệu thô (sau bước extract)
ETL_DATA_DIR = "data/etl" # Thư mục sẽ lưu dữ liệu sau khi transform

# Tạo thư mục ETL nếu chưa tồn tại
os.makedirs(ETL_DATA_DIR, exist_ok=True)

# ==============================================================================
# 3. TÌM VÀ ĐỌC TỆP TIN RAW MỚI NHẤT
# ==============================================================================

def get_latest_raw_file(raw_dir: str) -> str:
    """
    Tìm tệp tin CSV raw mới nhất trong thư mục được chỉ định.
    Tệp tin được coi là mới nhất dựa trên ngày tháng trong tên file (e.g., boxoffice_DDMMYYYY.csv).
    """
    files = [f for f in os.listdir(raw_dir) if f.startswith("boxoffice_") and f.endswith(".csv")]

    if not files:
        logging.error(f"Không tìm thấy tệp tin raw trong thư mục {raw_dir}. Vui lòng chạy extract_data.py trước.")
        raise FileNotFoundError(f"Không có tệp tin raw để xử lý trong {raw_dir}.")

    # Sắp xếp các tệp tin theo ngày tháng trong tên file (giảm dần) để lấy tệp mới nhất
    # Ví dụ: "boxoffice_01012023.csv" -> 01012023
    files.sort(key=lambda f: datetime.strptime(f.split('_')[1].split('.')[0], "%d%m%Y"), reverse=True)
    
    return os.path.join(raw_dir, files[0]) # Trả về đường dẫn đầy đủ của tệp mới nhất

try:
    latest_raw_file = get_latest_raw_file(RAW_DATA_DIR)
    logging.info(f"Đã tìm thấy tệp tin raw mới nhất: {latest_raw_file}")
except Exception as e:
    logging.error(f"Lỗi khi tìm tệp tin raw: {e}", exc_info=True)
    raise SystemExit("Lỗi tìm tệp raw – Quy trình Transform bị dừng.")

# Đọc dữ liệu từ tệp tin raw CSV
try:
    df_raw = pd.read_csv(latest_raw_file, encoding="utf-8-sig")
    logging.info(f"Đã đọc thành công {len(df_raw)} dòng dữ liệu từ {latest_raw_file}")
except Exception as e:
    logging.error(f"Lỗi khi đọc tệp tin raw: {e}", exc_info=True)
    raise SystemExit("Lỗi đọc dữ liệu raw – Quy trình Transform bị dừng.")

# ==============================================================================
# 4. CHUYỂN ĐỔI DỮ LIỆU (TRANSFORM)
# ==============================================================================
logging.info("Bắt đầu chuyển đổi dữ liệu (Transform)...")

try:
    df_transformed = df_raw.copy() # Tạo một bản sao để thực hiện các biến đổi

    # --- Làm sạch và chuyển đổi cột 'Doanh thu' ---
    # Ví dụ: "15.000.000đ" -> 15000000
    df_transformed['Doanh thu'] = df_transformed['Doanh thu'].astype(str) # Đảm bảo là chuỗi
    df_transformed['Doanh thu'] = df_transformed['Doanh thu'].str.replace('đ', '', regex=False) # Loại bỏ ký tự 'đ'
    df_transformed['Doanh thu'] = df_transformed['Doanh thu'].str.replace('.', '', regex=False) # Loại bỏ dấu phân cách hàng nghìn (dấu chấm)
    df_transformed['Doanh thu'] = df_transformed['Doanh thu'].str.replace(',', '', regex=False).str.strip() # Loại bỏ dấu phẩy (nếu có) và khoảng trắng
    # Chuyển đổi sang kiểu số, xử lý lỗi (coerce -> NaN), điền 0 cho NaN, rồi chuyển sang int
    df_transformed['Doanh thu'] = pd.to_numeric(df_transformed['Doanh thu'], errors='coerce').fillna(0).astype(int)

    # --- Làm sạch và chuyển đổi cột 'Vé' ---
    # Ví dụ: "15.000" -> 15000
    df_transformed['Vé'] = df_transformed['Vé'].astype(str)
    df_transformed['Vé'] = df_transformed['Vé'].str.replace('.', '', regex=False).str.strip()
    df_transformed['Vé'] = pd.to_numeric(df_transformed['Vé'], errors='coerce').fillna(0).astype(int)

    # --- Làm sạch và chuyển đổi cột 'Suất chiếu' ---
    # Ví dụ: "200" -> 200
    df_transformed['Suất chiếu'] = df_transformed['Suất chiếu'].astype(str)
    df_transformed['Suất chiếu'] = df_transformed['Suất chiếu'].str.replace('.', '', regex=False).str.strip()
    df_transformed['Suất chiếu'] = pd.to_numeric(df_transformed['Suất chiếu'], errors='coerce').fillna(0).astype(int)

    # --- Thêm cột 'Ngay_cap_nhat' (Ngày cập nhật) ---
    # Lấy ngày từ tên file raw (e.g., "boxoffice_01012023.csv" -> "01012023")
    filename_date_str = latest_raw_file.split('_')[1].split('.')[0]
    df_transformed['Ngay_cap_nhat'] = pd.to_datetime(filename_date_str, format="%d%m%Y").date()

    # --- Đổi tên cột để chuẩn hóa và dễ quản lý trong database ---
    df_transformed.rename(columns={
        "Tên phim": "Ten_phim",
        "Doanh thu": "Doanh_thu",
        "Vé": "So_luong_ve",
        "Suất chiếu": "So_suat_chieu"
    }, inplace=True) # inplace=True để áp dụng thay đổi trực tiếp lên DataFrame

    logging.info("Đã chuyển đổi dữ liệu thành công.")

except Exception as e:
    logging.error(f"Lỗi khi chuyển đổi dữ liệu: {e}", exc_info=True)
    raise SystemExit("Lỗi transform dữ liệu – Quy trình Transform bị dừng.")

# ==============================================================================
# 5. LƯU DỮ LIỆU ĐÃ CHUYỂN ĐỔI VÀO THƯ MỤC ETL
# ==============================================================================

# Tạo tên file output dựa trên ngày hiện tại
today_str = date.today().strftime("%d%m%Y")
etl_path = os.path.join(ETL_DATA_DIR, f"boxoffice_transformed_{today_str}.csv")

# Lưu DataFrame đã transform vào file CSV
df_transformed.to_csv(etl_path, index=False, encoding="utf-8-sig") # index=False để không ghi cột index
logging.info(f"Dữ liệu đã chuyển đổi được lưu tại: {os.path.abspath(etl_path)}")

# ==============================================================================
# 6. LOAD LOG VÀO DATABASE db_control.etl_log
# ==============================================================================
try:
    logging.info("Đang nạp log transform vào MySQL database...")

    # Thiết lập kết nối MySQL
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="", # Điền mật khẩu MySQL của bạn nếu có
        database="db_control", # Database để lưu trữ log
        port=3306
    )
    cursor = conn.cursor()

    # Tạo bảng etl_log nếu chưa tồn tại
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

    # Đọc từng dòng log từ file log hiện tại và chèn vào database
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" - ", 2)
            if len(parts) == 3:
                log_time_str, log_level, message = parts
                # Loại bỏ mili giây từ chuỗi thời gian log
                log_time = log_time_str.split(',')[0] 
                
                cursor.execute("""
                    INSERT INTO etl_log (log_time, log_level, message, source_file)
                    VALUES (%s, %s, %s, %s)
                """, (log_time, log_level, message, os.path.basename(log_file)))

    conn.commit() # Xác nhận các thay đổi vào database
    conn.close() # Đóng kết nối database
    logging.info("Đã nạp log transform vào bảng db_control.etl_log thành công.")

except Exception as e:
    logging.error(f"Lỗi khi nạp log transform vào database: {e}", exc_info=True)
    # Không dừng chương trình ở đây nếu việc ghi log vào DB thất bại,
    # ưu tiên việc tạo dữ liệu đã transform thành công.

logging.info("KẾT THÚC QUY TRÌNH TRANSFORM DỮ LIỆU BOXOFFICEVIETNAM")