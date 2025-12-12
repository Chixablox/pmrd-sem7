create procedure fn_dm_data_stg_to_dm_load(in start_date date, in end_date date)
begin
    delete from t_dm_task;

    insert into t_dm_task (
        source_id, sale_date,
        product_id, product_name,
        category_id, category_name,
        customer_email_id, customer_email,
        region_id, region_name,
        status_id, status_name,
        quantity, price, discount_percent, rating
    )
    select
        source_id, sale_date,
        product_id, product_name,
        category_id, category_name,
        customer_email_id, customer_email,
        region_id, region_name,
        status_id, status_name,
        quantity, price, discount_percent, rating
    from t_dm_stg_task
    where sale_date between start_date and end_date
       or sale_date is null;
end;
