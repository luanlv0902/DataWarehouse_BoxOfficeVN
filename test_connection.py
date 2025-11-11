from utils.db_connection import test_connection

for db in ["staging", "warehouse", "datamart", "control"]:
    test_connection(db)
