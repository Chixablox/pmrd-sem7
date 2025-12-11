create or replace function s_psql_dds.fn_dm_data_load(start_date date, end_date date)
returns void as $$
begin
    insert into s_psql_dds.t_dim_product(name)
    select distinct product_name from s_psql_dds.t_sql_source_structured
    where product_name is not null
      and product_name <> 'Unknown'
      and sale_date between start_date and end_date
    on conflict do nothing;

    insert into s_psql_dds.t_dim_category(name)
    select distinct category from s_psql_dds.t_sql_source_structured
    where category is not null
      and category <> 'Other'
      and sale_date between start_date and end_date
    on conflict do nothing;

    insert into s_psql_dds.t_dim_customer(email)
    select distinct customer_email from s_psql_dds.t_sql_source_structured
    where customer_email is not null
      and sale_date between start_date and end_date
    on conflict do nothing;

    insert into s_psql_dds.t_dim_region(name)
    select distinct region from s_psql_dds.t_sql_source_structured
    where region is not null
      and region <> 'Unknown'
      and sale_date between start_date and end_date
    on conflict do nothing;

    insert into s_psql_dds.t_dim_status(name)
    select distinct status from s_psql_dds.t_sql_source_structured
    where status is not null
      and sale_date between start_date and end_date
    on conflict do nothing;


    delete from s_psql_dds.t_dm_task
    where sale_date between start_date and end_date
       or sale_date is null;


    insert into s_psql_dds.t_dm_task (
        source_id, sale_date,
        product_id, product_name,
        category_id, category_name,
        customer_email_id, customer_email,
        region_id, region_name,
        status_id, status_name,
        quantity, price, discount_percent, rating
    )
    select
        s.id as source_id,
        s.sale_date,
        p.id as product_id,
        s.product_name,
        c.id as category_id,
        s.category,
        cu.id as customer_email_id,
        s.customer_email,
        r.id as region_id,
        s.region,
        st.id as status_id,
        s.status,
        s.quantity,
        s.price,
        s.discount_percent,
        s.rating
    from s_psql_dds.t_sql_source_structured s
    left join s_psql_dds.t_dim_product p on p.name = s.product_name
    left join s_psql_dds.t_dim_category c on c.name = s.category
    left join s_psql_dds.t_dim_customer cu on cu.email = s.customer_email
    left join s_psql_dds.t_dim_region r on r.name = s.region
    left join s_psql_dds.t_dim_status st on st.name = s.status
    where s.sale_date between start_date and end_date
       or s.sale_date is null;
end;
$$ language plpgsql;

