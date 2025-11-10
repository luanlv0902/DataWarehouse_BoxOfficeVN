import pandas as pd
from datetime import date

def extract_boxoffice_data():
    data = [
        {"film": "Mai", "revenue": 123456789, "date": str(date.today())},
        {"film": "Lật mặt 7", "revenue": 98765432, "date": str(date.today())}
    ]
    df = pd.DataFrame(data)
    file_path = f"data/boxoffice_{date.today()}.csv"
    df.to_csv(file_path, index=False)
    return file_path
