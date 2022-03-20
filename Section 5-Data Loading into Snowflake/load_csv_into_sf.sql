use role sysadmin;

use database ecommerce_db;

create schema ecommerce_dev;

create or replace table lineitem cluster by (L_SHIPDATE) as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" limit 1;
truncate table lineitem;

CREATE FILE FORMAT csv_load_format
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER =1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO';

-- Create a stage for lineitem table  ---
create stage stg_lineitem_csv_dev
storage_integration = aws_sf_data
url = '{your_bucket_name}/ecommerce_dev/lineitem/lineitem_csv/'
file_format = csv_load_format;

list @stg_lineitem_csv_dev;

copy into lineitem from @stg_lineitem_csv_dev ON_ERROR = ABORT_STATEMENT;


-- Validate the data----
select * from lineitem limit 10;

