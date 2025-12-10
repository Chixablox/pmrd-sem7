import pytest
from src.get_dataset import get_dataset
from unittest.mock import patch, MagicMock

def test_get_dataset():
    df = get_dataset(100)
    assert len(df) == 100
    assert len(df.columns) == 11

@patch('pandas.DataFrame.to_sql')
@patch('src.load_data_to_db.create_engine')
def test_load_data_to_db(mock_engine, mock_to_sql):
    from src.load_data_to_db import load_data_to_db

    df = get_dataset(10)
    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn

    load_data_to_db(df)

    assert mock_conn.execute.called
    assert mock_to_sql.called

@patch('src.fill_structured_table.create_engine')
def test_fill_structured_table(mock_engine):
    from src.fill_structured_table import fill_structured_table
    
    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
    
    fill_structured_table()
    assert mock_conn.execute.called