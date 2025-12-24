create table if not exists t_dm_stg_task (
    id bigint auto_increment primary key,
    sale_date date,
    product_id int,
    product_name varchar(255),
    category_id int,
    category_name varchar(100),
    customer_email_id int,
    customer_email varchar(255),
    region_id int,
    region_name varchar(50),
    status_id int,
    status_name varchar(50),
    quantity int,
    price decimal(10,2),
    discount_percent decimal(5,2),
    rating decimal(3,1)
);

