# from utils.db_connection import test_connection

# for db in ["staging", "warehouse", "datamart", "control"]:
#     test_connection(db)
from utils.db_connection import get_db_config

print(get_db_config("staging"))