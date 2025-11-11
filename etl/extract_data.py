# NOTE CÀI ĐẶT THƯ VIỆN CẦN THIẾT TRONG FILE: 
# pip install selenium webdriver-manager pandas beautifulsoup4

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
from webdriver_manager.chrome import ChromeDriverManager
import time

# === 1. KHỞI ĐỘNG QUY TRÌNH ===
os.makedirs("logs", exist_ok=True)
log_file = f"logs/etl_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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

# === 2. LẤY URL NGUỒN DỮ LIỆU ===
def is_valid_url(url: str) -> bool:
    pattern = r'^https?:\/\/[^\s\/$.?#].[^\s]*$'
    return re.match(pattern, url) is not None

if not is_valid_url(URL):
    logging.error("URL không hợp lệ. Gửi cảnh báo: 'URL không khả dụng'")
    raise SystemExit("URL không hợp lệ – Failed to retrieve source URL")
else:
    logging.info(f"URL hợp lệ: {URL}")

# === 3. GỬI REQUEST ===
try:
    logging.info("Đang khởi tạo trình duyệt Chrome (headless)...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    logging.info("Đang truy cập trang web...")
    driver.get(URL)
    time.sleep(6)
    html = driver.page_source
    driver.quit()
    logging.info("Đã tải thành công trang web.")
except Exception as e:
    logging.error(f"Request failed – Website unreachable: {e}", exc_info=True)
    raise SystemExit("Website unreachable")

# === 4. TRÍCH XUẤT DỮ LIỆU (Extract) ===
logging.info("Bắt đầu trích xuất dữ liệu (Extract)...")

try:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logging.error("HTML parsing error – Không tìm thấy bảng dữ liệu.")
        raise ValueError("Không tìm thấy dữ liệu trong HTML.")

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
        logging.warning("Không có dữ liệu trong bảng.")
        raise ValueError("Bảng rỗng.")
    else:
        logging.info(f"Trích xuất thành công {len(rows)} dòng dữ liệu.")

except Exception as e:
    logging.error(f"Lỗi khi trích xuất dữ liệu: {e}", exc_info=True)
    raise SystemExit("HTML parsing error")

# === LƯU DỮ LIỆU VÀO RAW ===
today_str = date.today().strftime("%d%m%Y")
os.makedirs("data/raw", exist_ok=True)
raw_path = f"data/raw/boxoffice_{today_str}.csv"
pd.DataFrame(rows).to_csv(raw_path, index=False, encoding="utf-8-sig")
logging.info(f"Dữ liệu đã lưu: {os.path.abspath(raw_path)}")

