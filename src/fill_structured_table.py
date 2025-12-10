from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from config import DATABASE_URL

BASE_DIR = Path(__file__).resolve().parent.parent


def _run_sql_file(conn, relative_path: str):
    sql_path = BASE_DIR / relative_path
    conn.execute(text(sql_path.read_text(encoding="utf-8")))


def fill_structured_table():
    engine = create_engine(DATABASE_URL)

    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    with engine.connect() as conn:
        conn.execute(text("create schema if not exists s_psql_dds"))
        _run_sql_file(conn, "sql/dds/table/t_sql_source_structured.sql")
        _run_sql_file(conn, "sql/dds/function/fn_etl_data_load.sql")
        conn.execute(text("truncate table s_psql_dds.t_sql_source_structured"))
        conn.execute(
            text("select s_psql_dds.fn_etl_data_load(:start_date, :end_date)"),
            {"start_date": start_date, "end_date": end_date},
        )
        conn.commit()
