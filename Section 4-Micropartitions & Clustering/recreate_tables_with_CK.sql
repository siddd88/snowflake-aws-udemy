use role sysadmin;

-- use schema "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000";

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

--- Re-create the table with a clustering key ---- 

create or replace table LINEITEM cluster by (L_SHIPDATE) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM";

create or replace table ORDERS cluster by (o_orderdate) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS";

create or replace table CUSTOMER as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."CUSTOMER";

create or replace table PARTSUPP cluster by (PS_SUPPKEY) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."PARTSUPP";

create or replace table SUPPLIER as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."SUPPLIER";

create or replace table PART as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."PART";

create or replace table NATION as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."NATION";

create or replace table REGION as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."REGION";

