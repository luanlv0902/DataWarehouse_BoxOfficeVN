# utils/db_connection.py
import json
import os
import mysql.connector

# Lấy đường dẫn gốc dự án
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "db_config.json")

# load config từ JSON
def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg

def get_db_config(db_key):
    cfg = load_config()
    mysql_cfg = cfg["mysql"].copy()
    db_map = cfg["databases"]
    
    if db_key not in db_map:
        raise KeyError(f"Unknown db_key: {db_key}")
    
    mysql_cfg["database"] = db_map[db_key]
    return mysql_cfg

# lấy ETL config từ db_control
def get_etl_config_from_db(key):
    """
    Lấy giá trị config từ bảng etl_config trong db_control
    """
    control_cfg = get_db_config("control")
    conn = mysql.connector.connect(**control_cfg)
    cur = conn.cursor()
    cur.execute("SELECT config_value FROM etl_config WHERE config_key=%s", (key,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None