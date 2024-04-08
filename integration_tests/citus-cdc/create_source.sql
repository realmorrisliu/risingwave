CREATE TABLE orders_rw (
    o_orderkey bigint,
    o_custkey bigint,
    o_orderstatus varchar,
    o_totalprice decimal,
    o_orderdate date,
    o_orderpriority varchar,
    o_clerk varchar,
    o_shippriority bigint,
    o_comment varchar,
    PRIMARY KEY (o_orderkey)
) WITH (
    connector = 'citus-cdc',
    hostname = 'citus-master',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.servers = 'citus-worker-1:5432,citus-worker-2:5432',
    database.name = 'mydb',
    schema.name = 'public',
    table.name = 'orders',
    slot.name = 'orders_dbz_slot',
);

CREATE TABLE citus_all_types(
    id int PRIMARY KEY,
    c_boolean boolean,
    c_smallint smallint,
    c_integer integer,
    c_bigint bigint,
    c_decimal decimal,
    c_real real,
    c_double_precision double precision,
    c_varchar varchar,
    c_bytea bytea,
    c_date date,
    c_time time,
    c_timestamp timestamp,
    c_timestamptz timestamptz,
    c_interval interval,
    c_jsonb jsonb,
    c_boolean_array boolean[],
    c_smallint_array smallint[],
    c_integer_array integer[],
    c_bigint_array bigint[],
    c_decimal_array decimal[],
    c_real_array real[],
    c_double_precision_array double precision[],
    c_varchar_array varchar[],
    c_bytea_array bytea[],
    c_date_array date[],
    c_time_array time[],
    c_timestamp_array timestamp[],
    c_timestamptz_array timestamptz[],
    c_jsonb_array jsonb[],
) WITH (
    connector = 'citus-cdc',
    hostname = 'citus-master',
    port = '5432',
    username = 'myuser',
    password = '123456',
    database.servers = 'citus-worker-1:5432,citus-worker-2:5432',
    database.name = 'mydb',
    schema.name = 'public',
    table.name = 'citus_all_types',
    slot.name = 'citus_all_types_dbz_slot',
);
