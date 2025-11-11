import json
from sqlalchemy import create_engine, text

# Đọc file config
with open("config/db_config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

MYSQL_CONFIG = config["mysql"]
DB_NAMES = config["databases"]

def get_engine(db_name):
    """
    Trả về engine SQLAlchemy tương ứng với tên database
    """
    db_full_name = DB_NAMES.get(db_name)
    if not db_full_name:
        raise ValueError(f"Không tìm thấy database '{db_name}' trong file config.")

    conn_str = f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{db_full_name}"
    return create_engine(conn_str)

def test_connection(db_name):
    """
    Kiểm tra kết nối tới 1 database cụ thể
    """
    try:
        engine = get_engine(db_name)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT NOW()")).fetchone()
            print(f"Kết nối thành công tới {db_name} – Server time: {result[0]}")
    except Exception as e:
        print(f"Lỗi kết nối tới {db_name}: {e}")
