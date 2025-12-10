create table if not exists s_psql_dds.t_sql_source_structured (
    id integer,
    sale_date date,
    product_name varchar(255),
    quantity integer,
    price numeric(10, 2),
    category varchar(100),
    customer_email varchar(255),
    region varchar(50),
    discount_percent numeric(5, 2),
    status varchar(50),
    rating numeric(3, 1)
);