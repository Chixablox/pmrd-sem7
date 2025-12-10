create or replace function s_psql_dds.fn_etl_data_load(start_date date, end_date date)
returns void as $$
begin
    delete from s_psql_dds.t_sql_source_structured;
    
    with src as (
        select
            nullif(trim(id), '')::int as id_clean,
            case 
                when sale_date is null or sale_date = 'invalid_date' then null
                else to_date(sale_date, 'YYYY-MM-DD')
            end as sale_date_clean,
            case 
                when product_name is null or trim(product_name) = '' then 'Unknown'
                else trim(product_name)
            end as product_name_clean,
            nullif(regexp_replace(quantity, '[^0-9\\.\\-]', '', 'g'), '')::numeric as quantity_clean,
            nullif(regexp_replace(price, '[^0-9\\.\\-]', '', 'g'), '')::numeric as price_clean,
            case 
                when category is null or trim(category) = '' then 'Other'
                else initcap(trim(category))
            end as category_clean,
            case 
                when customer_email is null or customer_email not like '%@%.%' then null
                else customer_email
            end as customer_email_clean,
            case 
                when region is null or trim(region) = '' then 'Unknown'
                else initcap(trim(region))
            end as region_clean,
            nullif(regexp_replace(discount_percent, '[^0-9\\.\\-]', '', 'g'), '')::numeric as discount_clean,
            case 
                when status is null or trim(status) = '' then 'unknown'
                when lower(trim(status)) in ('completed','pending','cancelled') then lower(trim(status))
                else 'unknown'
            end as status_clean,
            nullif(regexp_replace(rating, '[^0-9\\.\\-]', '', 'g'), '')::numeric as rating_clean
        from s_psql_dds.t_sql_source_unstructured
    )
    insert into s_psql_dds.t_sql_source_structured (
        id, sale_date, product_name, quantity, price, 
        category, customer_email, region, discount_percent, status, rating
    )
    select 
        id_clean,
        sale_date_clean,
        product_name_clean,
        case 
            when quantity_clean is null or quantity_clean < 0 or quantity_clean > 10000 then 0
            else quantity_clean::int
        end as quantity,
        case 
            when price_clean is null or price_clean <= 0 then 0
            else price_clean
        end as price,
        category_clean,
        customer_email_clean,
        region_clean,
        case 
            when discount_clean is null or discount_clean < 0 or discount_clean > 100 then 0
            else discount_clean
        end as discount_percent,
        status_clean,
        case
            when rating_clean is null or rating_clean < 0 or rating_clean > 5 then null
            else rating_clean
        end as rating
    from src
    where sale_date_clean between start_date and end_date
       or sale_date_clean is null;
end;
$$ language plpgsql;