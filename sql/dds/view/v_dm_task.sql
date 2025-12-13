create or replace view s_psql_dds.v_dm_task as
select
    id,
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
from s_psql_dds.t_dm_task;

