use role sysadmin;

create or replace database test_db_so;

create or replace schema test_schema;

use schema test_db_so.test_schema;

-- Create a table with no Search optimization feature enabled ----
create or replace table lineitem_no_so clone "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";
--  OR ---
create or replace table lineitem_no_so cluster by(l_shipdate) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";


-- Create a table and enable the Search optimization feature ----
create or replace table lineitem_so clone "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM";

select system$ESTIMATE_SEARCH_OPTIMIZATION_COSTS('lineitem_no_so');

alter table lineitem_so add search optimization;
show tables like '%lineitem_so%';

-- Point lookup query ---
select * from lineitem_no_so where l_orderkey='2412266214' limit 10;


-- Clear Result and WH Cache ---
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
alter warehouse prod_xl suspend;
alter warehouse prod_xl resume;