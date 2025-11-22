# etl/extract_data.py
import os
import re
import logging
import time
import pandas as pd
from datetime import datetime, date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

from utils.db_connection import get_db_config
from utils.log_to_db import push_log_file_to_db

# logging
os.makedirs("logs/extract", exist_ok=True)
log_file = f"logs/extract/extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", encoding="utf-8")
logging.getLogger().addHandler(logging.StreamHandler())

URL = "https://boxofficevietnam.com/"

def get_driver():
    options = Options()
    # headless new mode is stable for modern chrome
    options.add_argument("--headless=new")  
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=options)
    return driver

def scrape_to_csv():
    logging.info("Start extract")
    try:
        driver = get_driver()
        driver.get(URL)
        time.sleep(6)
        html = driver.page_source
        driver.quit()
    except Exception as e:
        logging.exception("Failed to load page")
        raise

    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logging.error("No table found on page")
        raise SystemExit("No table found")

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

    os.makedirs("data/raw", exist_ok=True)
    today_str = date.today().strftime("%d%m%Y")
    raw_path = f"data/raw/boxoffice_{today_str}.csv"
    pd.DataFrame(rows).to_csv(raw_path, index=False, encoding="utf-8-sig")
    logging.info(f"Wrote raw CSV: {raw_path}")
    return raw_path

if __name__ == "__main__":
    try:
        scrape_to_csv()
    finally:
        try:
            # ghi log vào db_control
            control_cfg = get_db_config("control")
            push_log_file_to_db(log_file, control_cfg)
            print("Đã ghi log vào db_control.")
        except Exception:
            logging.exception("Failed to push log to db_control")
            print("Lỗi ghi log vào db_control.")

