# etl_pipeline.py

# ==============================================================================
# 0. CÀI ĐẶT THƯ VIỆN CẦN THIẾT
# ==============================================================================
# Các thư viện này đã được cài đặt thông qua `pip install pandas mysql-connector-python`
# hoặc bạn có thể cần thêm `pip install ...` cho các thư viện khác mà script con sử dụng.

import subprocess  # Để chạy các script Python khác như tiến trình con
import logging     # Để ghi lại các thông báo trong quá trình chạy
from datetime import datetime # Để làm việc với ngày giờ
import os          # Để tương tác với hệ điều hành (tạo thư mục, đường dẫn)
import mysql.connector # Để kết nối và ghi log vào MySQL


# ==============================================================================
# 1. CẤU HÌNH LOGGING CHO PIPELINE CHÍNH
# ==============================================================================

# Tạo thư mục con 'pipeline' bên trong 'logs' nếu nó chưa tồn tại
os.makedirs("logs/pipeline", exist_ok=True)

# Định nghĩa tên file log cho pipeline, bao gồm ngày và giờ hiện tại
pipeline_log_file = f"logs/pipeline/etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

# Cấu hình logging cơ bản: ghi vào file và hiển thị trên console
logging.basicConfig(
    filename=pipeline_log_file,       # File để lưu log
    level=logging.INFO,               # Mức độ log thấp nhất được ghi (INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s", # Định dạng của mỗi dòng log
    encoding="utf-8"                  # Đảm bảo hỗ trợ tiếng Việt
)

# Thêm một handler để các thông báo log cũng được in ra màn hình (console)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

# Ghi thông báo khởi động quy trình ETL tổng thể
logging.info("BẮT ĐẦU QUY TRÌNH ETL TỔNG THỂ")

# ==============================================================================
# 2. ĐỊNH NGHĨA HÀM CHẠY SCRIPT CON
# ==============================================================================

def run_script(script_path: str, step_name: str) -> bool:
    """
    Thực thi một script Python bên ngoài và kiểm tra mã thoát của nó.
    Ghi log trạng thái và lỗi nếu có.
    """
    logging.info(f"[{step_name}] Đang bắt đầu thực thi script: {script_path}")
    try:
        result = subprocess.run(
            ["python", script_path],
            capture_output=True,
            text=True,
            check=True,
            encoding="utf-8",
            errors='replace' # <<< THÊM DÒNG NÀY VÀO ĐÂY
            # Hoặc errors='ignore' nếu bạn muốn bỏ qua hoàn toàn các ký tự lỗi
        )
        logging.info(f"[{step_name}] Thực thi script thành công.")
        if result.stdout:
            # Ghi ở mức DEBUG hoặc INFO để thấy output nếu cần debug,
            # nhưng thường không muốn quá nhiều output từ script con trong log chính.
            logging.debug(f"[{step_name}] STDOUT:\n{result.stdout.strip()}")
        if result.stderr:
            logging.warning(f"[{step_name}] STDERR (có thể là cảnh báo hoặc thông báo khác):\n{result.stderr.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"[{step_name}] Thực thi script thất bại với mã lỗi {e.returncode}.")
        # Lỗi giải mã có thể vẫn xảy ra ở đây nếu e.stdout hoặc e.stderr cũng chứa byte không hợp lệ,
        # nhưng subprocess.run đã cố gắng xử lý chúng rồi.
        logging.error(f"[{step_name}] Lỗi STDOUT:\n{e.stdout.strip() if e.stdout else 'Không có STDOUT'}")
        logging.error(f"[{step_name}] Lỗi STDERR:\n{e.stderr.strip() if e.stderr else 'Không có STDERR'}")
        return False
    except FileNotFoundError:
        logging.error(f"[{step_name}] Lỗi: Không tìm thấy script tại đường dẫn: {script_path}")
        return False
    except Exception as e:
        logging.error(f"[{step_name}] Lỗi không xác định khi chạy script: {e}", exc_info=True)
        return False

# ==============================================================================
# 3. ĐỊNH NGHĨA VÀ CHẠY CHUỖI CÁC BƯỚC ETL (WORKFLOW)
# ==============================================================================

# Định nghĩa đường dẫn tương đối đến các script con
# (từ vị trí của etl_pipeline.py)
EXTRACT_SCRIPT = "etl/extract_data.py"
TRANSFORM_SCRIPT = "etl/transform_data.py"
LOAD_DW_SCRIPT = "etl/load_datawarehouse.py"
LOAD_DM_SCRIPT = "etl/load_datamart.py"
# AGGREGATE_SCRIPT = "etl/aggreagre_data.py" # Có thể thêm vào nếu bạn có bước tổng hợp riêng

# Định nghĩa trình tự các bước trong pipeline
etl_steps = [
    {"name": "EXTRACT", "script": EXTRACT_SCRIPT},
    {"name": "TRANSFORM", "script": TRANSFORM_SCRIPT},
    {"name": "LOAD_DATA_WAREHOUSE", "script": LOAD_DW_SCRIPT},
    {"name": "LOAD_DATA_MART", "script": LOAD_DM_SCRIPT},
    # Bạn có thể thêm các bước khác vào đây theo thứ tự logic
    # {"name": "AGGREGATE_DATA", "script": AGGREGATE_SCRIPT},
]

# Biến cờ để theo dõi trạng thái thành công của toàn bộ pipeline
pipeline_overall_success = True

# Chạy từng bước trong pipeline
for step in etl_steps:
    # Gọi hàm run_script để thực thi script con cho từng bước
    if not run_script(step["script"], step["name"]):
        # Nếu một bước thất bại, ghi thông báo CRITICAL và dừng toàn bộ pipeline
        logging.critical(f"QUY TRÌNH ETL DỪNG LẠI: Bước '{step['name']}' thất bại.")
        pipeline_overall_success = False
        break # Thoát khỏi vòng lặp, không chạy các bước tiếp theo

# Ghi thông báo cuối cùng về trạng thái của toàn bộ pipeline
if pipeline_overall_success:
    logging.info("QUY TRÌNH ETL TỔNG THỂ ĐÃ HOÀN THÀNH THÀNH CÔNG.")
else:
    logging.error("QUY TRÌNH ETL TỔNG THỂ ĐÃ THẤT BẠI TẠI MỘT SỐ BƯỚC.")

# ==============================================================================
# 4. LOAD LOG CỦA PIPELINE CHÍNH VÀO DATABASE db_control.etl_log
# ==============================================================================
try:
    logging.info("Đang nạp log pipeline chính vào MySQL database...")

    # Thiết lập kết nối MySQL để ghi log
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="", # ĐIỀN MẬT KHẨU MYSQL CỦA BẠN NẾU CÓ
        database="db_control",
        port=3306
    )
    cursor = conn.cursor()

    # Tạo bảng etl_log nếu chưa tồn tại (đảm bảo bảng luôn sẵn sàng)
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

    # Đọc từng dòng log từ file log của pipeline hiện tại
    with open(pipeline_log_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" - ", 2) # Tách dòng log thành 3 phần
            if len(parts) == 3:
                log_time_str, log_level, message = parts
                # Loại bỏ phần mili giây từ chuỗi thời gian log trước khi chèn vào DB
                log_time = log_time_str.split(',')[0] 
                
                # Chèn dòng log vào bảng etl_log
                cursor.execute("""
                    INSERT INTO etl_log (log_time, log_level, message, source_file)
                    VALUES (%s, %s, %s, %s)
                """, (log_time, log_level, message, os.path.basename(pipeline_log_file)))

    conn.commit() # Xác nhận các thay đổi vào database
    conn.close() # Đóng kết nối database
    logging.info("Đã nạp log pipeline chính vào bảng db_control.etl_log thành công.")

except Exception as e:
    # Ghi lỗi nếu không thể nạp log vào database, nhưng không dừng toàn bộ chương trình
    logging.error(f"Lỗi khi nạp log pipeline chính vào database: {e}", exc_info=True)

logging.info("KẾT THÚC QUY TRÌNH ETL TỔNG THỂ")