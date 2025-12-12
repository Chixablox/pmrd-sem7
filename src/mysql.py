from pathlib import Path
import argparse
import pandas as pd
from sqlalchemy import create_engine, text

from config import DATABASE_URL, MYSQL_URL

BASE_DIR = Path(__file__).resolve().parent.parent


def run_sql_file(conn, relative_path):
    sql_path = BASE_DIR / relative_path
    conn.execute(text(sql_path.read_text(encoding="utf-8")))


def dm_to_mysql(start_date, end_date):
    pg_engine = create_engine(DATABASE_URL)
    mysql_engine = create_engine(MYSQL_URL)

    query = text(
        """
        select
            id,
            source_id,
            sale_date,
            product_id,
            product_name,
            category_id,
            category_name,
            customer_email_id,
            customer_email,
            region_id,
            region_name,
            status_id,
            status_name,
            quantity,
            price,
            discount_percent,
            rating
        from s_psql_dds.v_dm_task
        """
    )

    df = pd.read_sql(query, pg_engine)
    
    with mysql_engine.begin() as conn:
        run_sql_file(conn, "sql/mysql/table/t_dm_task.sql")
        run_sql_file(conn, "sql/mysql/table/t_dm_stg_task.sql")
        try:
            run_sql_file(conn, "sql/mysql/function/fn_dm_data_stg_to_dm_load.sql")
        except:
            print("Процедура уже есть")
        conn.execute(text("truncate table t_dm_stg_task"))

    if not df.empty:
        df.to_sql("t_dm_stg_task", mysql_engine, if_exists="append", index=False)

    with mysql_engine.begin() as conn:
        conn.execute(text("CALL fn_dm_data_stg_to_dm_load(:start, :end)"), 
                        {"start": start_date, "end": end_date})

def main():
    parser = argparse.ArgumentParser(description="ETL: DM to MySQL")
    parser.add_argument("start_date", help="Start date YYYY-MM-DD")
    parser.add_argument("end_date", help="End date YYYY-MM-DD")
    args = parser.parse_args()
    
    dm_to_mysql(args.start_date, args.end_date)

if __name__ == "__main__":
    main()
    