use role sysadmin;

use schema ecommerce_db.ecommerce_dev;

CREATE OR REPLACE FILE FORMAT json_load_format TYPE = 'JSON' ;


-- Create a stage for lineitem table  ---
create stage stg_lineitem_json_dev
storage_integration = aws_sf_data
url = '{your_bucket_name}/ecommerce_dev/lineitem/lineitem_json/'
file_format = json_load_format;

-- List all the files to check before loading ---
list @stg_lineitem_json_dev;

-- Select the data directly from staged location to validate the data before loading ---
select $1 from @stg_lineitem_json_dev limit 10;

-- insert into lineitem directly from staged location --- 
select 
    $1:L_ORDERKEY,
    $1:L_PARTKEY,
    $1:L_SUPPKEY,
    $1:L_LINENUMBER,
    $1:L_QUANTITY,
    $1:L_EXTENDEDPRICE,
    $1:L_DISCOUNT,
    $1:L_TAX,
    $1:L_RETURNFLAG,
    $1:L_LINESTATUS,
    $1:L_SHIPDATE,
    $1:L_COMMITDATE,
    $1:L_RECEIPTDATE,
    $1:L_SHIPINSTRUCT,
    $1:L_SHIPMODE,
    $1:L_COMMENT
from 
    @stg_lineitem_json_dev ;
-- limit 10;


-- Create a raw table with variant datatype column --- 
create table lineitem_raw_json (src variant );

-- Ingest data into the raw table from staged location --- 
copy into lineitem_raw_json from @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;


-- insert into the structured table from raw table --- 
select 
    SRC:L_ORDERKEY,
    SRC:L_PARTKEY,
    SRC:L_SUPPKEY,
    SRC:L_LINENUMBER,
    SRC:L_QUANTITY,
    SRC:L_EXTENDEDPRICE,
    SRC:L_DISCOUNT,
    SRC:L_TAX,
    SRC:L_RETURNFLAG,
    SRC:L_LINESTATUS,
    SRC:L_SHIPDATE,
    SRC:L_COMMITDATE,
    SRC:L_RECEIPTDATE,
    SRC:L_SHIPINSTRUCT,
    SRC:L_SHIPMODE,
    SRC:L_COMMENT
from 
    lineitem_raw_json ;
-- limit 10;
