create table if not exists s_psql_dds.t_dim_status (
    id serial primary key,
    name varchar(50) unique
);

