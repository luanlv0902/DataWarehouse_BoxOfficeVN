# etl/extract_data.py
import os
import re
import logging
import time
import pandas as pd
from datetime import datetime, date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config, get_etl_config_from_db
from utils.log_to_db import push_log_file_to_db

# 1. Khởi tạo logging + Tạo file log
log_dir = "logs/extract"
os.makedirs(log_dir, exist_ok=True)
log_file = f"{log_dir}/extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

URL = "https://boxofficevietnam.com/"

  # 2. Khởi tạo Chrome Driver (headless)
def get_driver():
  
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    driver = webdriver.Chrome(options=options)
    return driver

def scrape_to_csv():
    logging.info("Start extract")
    
    try:
        # 3. Lấy URL nguồn dữ liệu
        driver = get_driver()                                 
        logging.info("Đang lấy dữ liệu từ URL nguồn: https://boxofficevietnam.com/")
        driver.get(URL)
        time.sleep(6)                                       
        html = driver.page_source
        driver.quit()
    except Exception as e:
        logging.exception("Failed to load page")                
        raise

    # 4. Parse HTML bằng BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    
    # 5. Tìm table trong trang
    table = soup.find("table")
    if not table:
        logging.error("No table found on page")                  
        raise SystemExit("No table found")

    # 6. Duyệt từng <tr> trong tbody → trích xuất dữ liệu
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
        logging.error("No rows extracted")                       
        raise SystemExit("No rows")

    # 7. Lấy config lưu raw từ db_control → nếu không có thì dùng mặc định
    raw_dir = get_etl_config_from_db("raw_data_path") or "data/raw"
    os.makedirs(raw_dir, exist_ok=True)
    today_str = date.today().strftime("%d%m%Y")
   
    
    # 8. Tạo file boxoffice_DDMMYYYY.csv, Ghi DataFrame → CSV 
    raw_path = os.path.join(raw_dir, f"boxoffice_{today_str}.csv")
    pd.DataFrame(rows).to_csv(raw_path, index=False, encoding="utf-8-sig")
    logging.info(f"Wrote raw CSV: {raw_path}")                  
    return raw_path


if __name__ == "__main__":
    try:
        scrape_to_csv()
    finally:
        # 9. Push log file lên DB control
        try:
            logging.info("Bắt đầu đẩy log file lên DB control...")
            control_cfg = get_db_config("control")              # Lấy config DB control
            push_log_file_to_db(log_file, control_cfg)          # Thực hiện đẩy log
            print("Đã ghi log vào db_control.")
            logging.info("Đã đẩy log thành công lên DB control")
        except Exception:
            logging.exception("Failed to push log to db_control")  
            print("Lỗi ghi log vào db_control.")