from pathlib import Path
from sqlalchemy import create_engine, text
from config import DATABASE_URL

BASE_DIR = Path(__file__).resolve().parent.parent


def _run_sql_file(conn, relative_path: str):
    sql_path = BASE_DIR / relative_path
    conn.execute(text(sql_path.read_text(encoding="utf-8")))


def load_data_to_db(df):
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        conn.execute(text("create schema if not exists s_psql_dds"))
        _run_sql_file(conn, "sql/dds/table/t_sql_source_unstructured.sql")
        conn.execute(text("truncate table s_psql_dds.t_sql_source_unstructured"))
        conn.commit()

    df.to_sql(
        't_sql_source_unstructured',
        engine,
        schema='s_psql_dds',
        if_exists='append',
        index=False
    )
