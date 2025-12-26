create or replace function s_psql_dds.fn_dq_checks_load(start_dt date, end_dt date)
returns void as $$
declare
    total_src bigint;
    total_v bigint;
    nulls_product bigint;
    total_records bigint;
    nulls_pct numeric;
    neg_count bigint;
    dup_count bigint;
    invalid_status_count bigint;
    sum_detail numeric;
    sum_total numeric;
begin
    -- 1) Правильность: сравнение количества записей между источником и витриной
    select count(*) into total_src 
    from s_psql_dds.t_sql_source_structured s
    where s.sale_date between start_dt and end_dt;

    select count(*) into total_v 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt;

    if total_src = total_v then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('correctness', 'v_dm_task', 'passed', 
               format('Counts match: source=%s, view=%s', total_src, total_v));
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('correctness', 'v_dm_task', 'failed',
               format('Count mismatch: source=%s, view=%s', total_src, total_v));
    end if;

    -- 2) Полнота: процент пропусков в критических полях (product_id)
    select count(*) into total_records 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt;

    select count(*) into nulls_product 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt
      and v.product_id is null;

    if total_records = 0 then
        nulls_pct := 0;
    else
        nulls_pct := (nulls_product::numeric / total_records::numeric) * 100;
    end if;

    if nulls_pct <= 1 then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('completeness', 'v_dm_task', 'passed', 
               'Null product_id percentage: ' || round(nulls_pct, 2) || '%');
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('completeness', 'v_dm_task', 'failed', 
               'Null product_id percentage: ' || round(nulls_pct, 2) || '% (threshold: 1%)');
    end if;

    -- 3) Непротиворечивость: сумма детальных записей <= общей суммы по группе
    select coalesce(sum(v.price * v.quantity), 0) into sum_detail
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt
      and v.product_id is not null;

    select coalesce(sum(v.price * v.quantity), 0) into sum_total
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt;

    if sum_detail <= sum_total then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('consistency', 'v_dm_task', 'passed', 
               'Sum consistency check passed: detail_sum=' || round(sum_detail, 2) || ', total_sum=' || round(sum_total, 2));
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('consistency', 'v_dm_task', 'failed', 
               'Sum inconsistency: detail_sum=' || round(sum_detail, 2) || ' > total_sum=' || round(sum_total, 2));
    end if;

    -- Дополнительная проверка непротиворечивости: отсутствие отрицательных значений
    select count(*) into neg_count 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt
      and (v.quantity < 0 or v.price < 0);

    if neg_count = 0 then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('consistency', 'v_dm_task', 'passed', 
               'No negative quantity or price values');
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('consistency', 'v_dm_task', 'failed', 
               'Found ' || neg_count || ' records with negative quantity or price');
    end if;

    -- 4) Уникальность: отсутствие дубликатов по ключевому полю id
    select count(*) - count(distinct id) into dup_count 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt;

    if dup_count = 0 then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('uniqueness', 'v_dm_task', 'passed', 
               'No duplicate id values');
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('uniqueness', 'v_dm_task', 'failed', 
               'Found ' || dup_count || ' duplicate id values');
    end if;

    -- 5) Валидность: статусы соответствуют допустимым значениям
    select count(*) into invalid_status_count 
    from s_psql_dds.v_dm_task v
    where v.sale_date between start_dt and end_dt
      and v.status_name is not null
      and lower(trim(v.status_name)) not in ('new', 'processed', 'shipped', 'cancelled');

    if invalid_status_count = 0 then
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('validity', 'v_dm_task', 'passed', 
               'All status values are valid');
    else
        insert into s_psql_dds.t_dq_check_results(check_type, table_name, status, error_message)
        values('validity', 'v_dm_task', 'failed', 
               'Found ' || invalid_status_count || ' records with invalid status values');
    end if;

end;
$$ language plpgsql;