create table if not exists s_psql_dds.t_dm_task (
    id bigserial primary key,
    sale_date date,
    product_id integer,
    product_name varchar(255),
    category_id integer,
    category_name varchar(100),
    customer_email_id integer,
    customer_email varchar(255),
    region_id integer,
    region_name varchar(50),
    status_id integer,
    status_name varchar(50),
    quantity integer,
    price numeric(10, 2),
    discount_percent numeric(5, 2),
    rating numeric(3, 1)
);

