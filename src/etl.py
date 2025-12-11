from src.get_dataset import get_dataset
from src.load_data_to_db import load_data_to_db
from src.fill_structured_table import fill_structured_table
from src.fill_dm_table import fill_dm_table


def etl():

    df = get_dataset(1000)

    load_data_to_db(df)

    fill_structured_table()

    fill_dm_table()
    