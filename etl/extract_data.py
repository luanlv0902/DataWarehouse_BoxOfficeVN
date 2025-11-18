import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
from datetime import date, datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import mysql.connector

# === 1. SETUP LOGGING ===
os.makedirs("logs/extract", exist_ok=True)
os.makedirs("data/raw", exist_ok=True)

log_file = f"logs/extract/etl_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

logging.info("BẮT ĐẦU QUY TRÌNH ETL DỮ LIỆU BOXOFFICEVIETNAM")

URL = "https://boxofficevietnam.com/"

# === 2. KIỂM TRA URL ===
def is_valid_url(url: str) -> bool:
    pattern = r'^https?:\/\/[^\s\/$.?#].[^\s]*$'
    return re.match(pattern, url) is not None

if not is_valid_url(URL):
    logging.error("URL không hợp lệ.")
    raise SystemExit("URL không hợp lệ.")
logging.info(f"URL hợp lệ: {URL}")

# === 3. KHỞI ĐỘNG SELENIUM ===
try:
    logging.info("Khởi tạo Chrome headless...")

    options = Options()
    options.add_argument("--headless=new")  
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=options)

    logging.info("Đang truy cập trang web...")
    driver.get(URL)

    # Chờ bảng dữ liệu xuất hiện
    wait = WebDriverWait(driver, 15)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))

    html = driver.page_source
    driver.quit()

    logging.info("Đã tải thành công trang web.")

except Exception as e:
    logging.error(f"Request failed – Website unreachable: {e}", exc_info=True)
    raise SystemExit("Website unreachable")

# === 4. EXTRACT DATA ===
logging.info("Đang trích xuất dữ liệu...")

try:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")

    if not table:
        logging.error("Không tìm thấy bảng dữ liệu.")
        raise ValueError("HTML không chứa bảng dữ liệu.")

    rows = []
    for tr in table.select("tbody tr"):
        cols = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cols) >= 4:
            rows.append({
                "Tên phim": cols[0],
                "Doanh thu": cols[1],
                "Vé": cols[2],
                "Suất chiếu": cols[3]
            })

    if not rows:
        logging.error("Không có dữ liệu trong bảng.")
        raise ValueError("Bảng dữ liệu rỗng.")

    logging.info(f"Trích xuất được {len(rows)} dòng.")

except Exception as e:
    logging.error(f"Lỗi Extract: {e}", exc_info=True)
    raise SystemExit("HTML parsing error")

# === 5. LƯU DỮ LIỆU RAW ===
today_str = date.today().strftime("%d%m%Y")
raw_path = f"data/raw/boxoffice_{today_str}.csv"

pd.DataFrame(rows).to_csv(raw_path, index=False, encoding="utf-8-sig")
logging.info(f"Dữ liệu đã lưu vào: {os.path.abspath(raw_path)}")

# === 6. LOAD LOG VÀO DATABASE ===
try:
    logging.info("Đang ghi log vào MySQL...")

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="db_control",
        port=3306
    )
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

    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" - ", 2)
            if len(parts) == 3:
                log_time, log_level, message = parts
                log_time = log_time.split(",")[0]  # bỏ ms
                cursor.execute("""
                    INSERT INTO etl_log (log_time, log_level, message, source_file)
                    VALUES (%s, %s, %s, %s)
                """, (log_time, log_level, message, os.path.basename(log_file)))

    conn.commit()
    conn.close()

    logging.info("Ghi log vào MySQL thành công.")

except Exception as e:
    logging.error(f"Lỗi ghi log vào MySQL: {e}", exc_info=True)