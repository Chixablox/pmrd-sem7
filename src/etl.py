from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table

def etl():
    print("=== ETL Process Started ===")
    
    print("1. Генерация синтетических данных...")
    df = get_dataset(1000)
    
    print("2. Загрузка данных в unstructured таблицу...")
    load_data_to_db(df)
    
    print("3. Трансформация и загрузка в structured таблицу...")
    fill_structured_table()
    
    print("=== ETL Process Completed ===")