create table if not exists s_psql_dds.t_dim_category (
    id serial primary key,
    name varchar(100) unique
);

