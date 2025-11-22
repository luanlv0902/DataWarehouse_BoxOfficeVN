from etl.extract_data import scrape_to_csv
from etl.load_staging import run_staging_load
from etl.transform_data import transform_latest_to_csv
from etl.load_datawarehouse import run_warehouse_load
from etl.aggregate_data import aggregate_for_datamart
from etl.load_datamart import load_to_datamart

if __name__ == "__main__":
    print("Starting full ETL pipeline")
    scrape_to_csv()
    run_staging_load()
    transform_latest_to_csv()
    run_warehouse_load()
    aggregate_for_datamart()
    load_to_datamart()
    print("ETL pipeline finished")