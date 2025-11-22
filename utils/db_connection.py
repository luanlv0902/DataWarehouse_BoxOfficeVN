# utils/db_connection.py
import json
import os

# Lấy đường dẫn gốc dự án
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.path.join(BASE_DIR, "config", "db_config.json")

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg

def get_db_config(db_key):
    cfg = load_config()
    mysql = cfg["mysql"].copy()
    db_map = cfg["databases"]
    
    if db_key not in db_map:
        raise KeyError(f"Unknown db_key: {db_key}")
    
    mysql["database"] = db_map[db_key]
    return mysql
