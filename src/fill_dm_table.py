from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import create_engine, text

from config import DATABASE_URL

BASE_DIR = Path(__file__).resolve().parent.parent


def run_sql_file(conn, relative_path: str) -> None:
    sql_path = BASE_DIR / relative_path
    conn.execute(text(sql_path.read_text(encoding="utf-8")))


def fill_dm_table():
    engine = create_engine(DATABASE_URL)
    start_date = (datetime.now() - timedelta(days=183)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    with engine.connect() as conn:
        run_sql_file(conn, "sql/dds/table/t_dim_product.sql")
        run_sql_file(conn, "sql/dds/table/t_dim_category.sql")
        run_sql_file(conn, "sql/dds/table/t_dim_customer.sql")
        run_sql_file(conn, "sql/dds/table/t_dim_region.sql")
        run_sql_file(conn, "sql/dds/table/t_dim_status.sql")
        run_sql_file(conn, "sql/dds/table/t_dm_task.sql")

        run_sql_file(conn, "sql/dds/function/fn_dm_data_load.sql")
        run_sql_file(conn, "sql/dds/view/v_dm_task.sql")
        conn.execute(
            text("select s_psql_dds.fn_dm_data_load(:start_date, :end_date)"),
            {"start_date": start_date, "end_date": end_date},
        )
        run_sql_file(conn, "sql/dds/table/t_dq_check_results.sql")
        run_sql_file(conn, "sql/dds/function/fn_dq_checks_load.sql")
        conn.execute(
            text("select s_psql_dds.fn_dq_checks_load(:start_date, :end_date)"),
            {"start_date": start_date, "end_date": end_date},
        )
        conn.commit()
