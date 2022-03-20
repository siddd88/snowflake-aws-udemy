use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

-- Extract/Unload data  ---
copy into s3://sid-snowflake-data/unloaded_data/lineitem/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;

-- Extract/Unload data using partition by ---
copy into s3://sid-snowflake-data/unloaded_data/lineitem_partitioned/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 100000
)
partition by L_SHIPDATE
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;



copy into s3://sid-snowflake-data/unloaded_data/lineitem_parquet/
from
(
  select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = parquet_format;



copy into s3://sid-snowflake-data/ecommerce_dev/lineitem/lineitem_json/
from
(
  select 
  object_construct(
  'L_ORDERKEY',L_ORDERKEY,
  'L_PARTKEY',L_PARTKEY,
  'L_SUPPKEY',L_SUPPKEY,
  'L_LINENUMBER',L_LINENUMBER,
   'L_QUANTITY',L_QUANTITY,
   'L_EXTENDEDPRICE',L_EXTENDEDPRICE,
  'L_DISCOUNT',L_DISCOUNT,
    'L_TAX',L_TAX,
    'L_RETURNFLAG',L_RETURNFLAG,
    'L_LINESTATUS',L_LINESTATUS,
    'L_SHIPDATE',L_SHIPDATE,
    'L_COMMITDATE',L_COMMITDATE,
    'L_RECEIPTDATE',L_RECEIPTDATE,
    'L_SHIPINSTRUCT',L_SHIPINSTRUCT,
    'L_SHIPMODE',L_SHIPMODE,
    'L_COMMENT',L_COMMENT
  )
  from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
  limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = json_unload_format;