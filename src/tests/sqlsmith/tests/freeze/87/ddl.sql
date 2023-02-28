CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT (sq_2.col_0 % ((SMALLINT '278') - (SMALLINT '32767'))) AS col_0, sq_2.col_0 AS col_1 FROM (SELECT t_1.id AS col_0, t_1.id AS col_1, t_0.c16 AS col_2, ((120131714) - CAST(false AS INT)) AS col_3 FROM alltypes1 AS t_0 FULL JOIN person AS t_1 ON t_0.c9 = t_1.city WHERE t_0.c1 GROUP BY t_0.c16, t_1.date_time, t_0.c9, t_1.email_address, t_1.id, t_0.c8, t_0.c3, t_0.c15 HAVING true) AS sq_2 GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m1 AS WITH with_0 AS (SELECT sq_3.col_0 AS col_0, (FLOAT '196') AS col_1, (substr(sq_3.col_0, ((INT '90') / ((INT '28'))), (INT '233'))) AS col_2, (BIGINT '470') AS col_3 FROM (SELECT (split_part((lower((OVERLAY(('B99HY2ZSet') PLACING ('z9UMq3hzpw') FROM ((INT '895')) FOR t_1.o_shippriority)))), t_1.o_clerk, (INT '-1361077303'))) AS col_0 FROM orders AS t_1 JOIN region AS t_2 ON t_1.o_clerk = t_2.r_comment WHERE false GROUP BY t_2.r_comment, t_2.r_regionkey, t_1.o_custkey, t_2.r_name, t_1.o_clerk, t_1.o_shippriority) AS sq_3 GROUP BY sq_3.col_0) SELECT (concat_ws('EW4qrJUQAY', 'xamWr3Pk12', (replace('Yp0psbNztg', 'E5U1E0Wn63', '4kHgAFPLaf')))) AS col_0, (SMALLINT '972') AS col_1, (FLOAT '78') AS col_2, ((SMALLINT '846') <> (INT '299')) AS col_3 FROM with_0;
CREATE MATERIALIZED VIEW m2 AS SELECT 'wFcp0kpx4m' AS col_0, (INT '2147483647') AS col_1, sq_1.col_2 AS col_2 FROM (SELECT (BIGINT '9223372036854775807') AS col_0, t_0.reserve AS col_1, t_0.reserve AS col_2 FROM auction AS t_0 GROUP BY t_0.reserve HAVING true) AS sq_1 GROUP BY sq_1.col_2 HAVING false;
CREATE MATERIALIZED VIEW m3 AS SELECT false AS col_0, hop_0.c16 AS col_1, (INTERVAL '-604800') AS col_2 FROM hop(alltypes2, alltypes2.c11, INTERVAL '604800', INTERVAL '55641600') AS hop_0 WHERE hop_0.c1 GROUP BY hop_0.c7, hop_0.c14, hop_0.c11, hop_0.c16, hop_0.c3, hop_0.c10, hop_0.c13, hop_0.c2 HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT (t_0.o_orderdate - t_0.o_shippriority) AS col_0 FROM orders AS t_0 JOIN customer AS t_1 ON t_0.o_shippriority = t_1.c_nationkey GROUP BY t_0.o_orderdate, t_0.o_comment, t_1.c_mktsegment, t_0.o_shippriority;
CREATE MATERIALIZED VIEW m6 AS SELECT TIMESTAMP '2022-09-02 17:25:20' AS col_0, TIMESTAMP '2022-09-02 17:25:20' AS col_1, tumble_0.item_name AS col_2 FROM tumble(auction, auction.date_time, INTERVAL '9') AS tumble_0 GROUP BY tumble_0.item_name, tumble_0.date_time HAVING true;
CREATE MATERIALIZED VIEW m7 AS SELECT TIME '17:25:20' AS col_0, (BIGINT '-6472192646849774225') AS col_1, (REAL '960') AS col_2, ((FLOAT '82')) AS col_3 FROM customer AS t_0 JOIN customer AS t_1 ON t_0.c_phone = t_1.c_comment WHERE ((REAL '914') <> (FLOAT '41')) GROUP BY t_1.c_phone, t_1.c_acctbal, t_0.c_comment;
CREATE MATERIALIZED VIEW m8 AS SELECT t_1.description AS col_0 FROM m0 AS t_0 FULL JOIN auction AS t_1 ON t_0.col_1 = t_1.id WHERE false GROUP BY t_1.initial_bid, t_1.item_name, t_1.description, t_0.col_0;
CREATE MATERIALIZED VIEW m9 AS SELECT hop_0.c15 AS col_0, ARRAY[DATE '2022-08-25'] AS col_1 FROM hop(alltypes1, alltypes1.c11, INTERVAL '573024', INTERVAL '42976800') AS hop_0 WHERE hop_0.c1 GROUP BY hop_0.c7, hop_0.c14, hop_0.c10, hop_0.c9, hop_0.c1, hop_0.c6, hop_0.c15;
