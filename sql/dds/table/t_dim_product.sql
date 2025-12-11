create table if not exists s_psql_dds.t_dim_product (
    id serial primary key,
    name varchar(255) unique
);

