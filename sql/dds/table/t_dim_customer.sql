create table if not exists s_psql_dds.t_dim_customer (
    id serial primary key,
    email varchar(255) unique
);

