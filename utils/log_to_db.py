# utils/log_to_db.py
import os
import mysql.connector
from datetime import datetime

def ensure_etl_log_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            log_time DATETIME,
            log_level VARCHAR(20),
            message TEXT,
            source_file VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.close()
    conn.commit()

def push_log_file_to_db(log_file_path, db_config):
    if not os.path.exists(log_file_path):
        print(f"Log file not found: {log_file_path}")
        return

    conn = mysql.connector.connect(**db_config)
    ensure_etl_log_table(conn)
    cur = conn.cursor()

    with open(log_file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" - ", 2)
            if len(parts) == 3:
                log_time_str, log_level, message = parts
                # log_time_str like "2025-11-04 21:45:16,123"
                log_time = log_time_str.split(",")[0]
                try:
                    # Try parse to ensure format
                    dt = datetime.fromisoformat(log_time)
                except Exception:
                    dt = None
                cur.execute("""
                    INSERT INTO etl_log (log_time, log_level, message, source_file)
                    VALUES (%s, %s, %s, %s)
                """, (log_time, log_level, message, os.path.basename(log_file_path)))
    conn.commit()
    cur.close()
    conn.close()
    print("Pushed logs to DB Control.")