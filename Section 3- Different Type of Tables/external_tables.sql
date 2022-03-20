use role sysadmin;

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

copy into s3://sid-snowflake-data/ecommerce_dev/orders/
from
(
  select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS" limit 1000000
)
storage_integration=aws_sf_data
single=false
file_format = csv_load_format;

create stage stg_orders_csv
storage_integration = aws_sf_data
url = 's3://sid-snowflake-data/ecommerce_dev/orders/'
file_format = csv_load_format;


list @stg_orders_csv;

create or replace external table orders_ext 
with location = @stg_orders_csv file_format = csv_load_format;

CREATE EXTERNAL TABLE orders_ext(
 date_part date AS TO_DATE(SPLIT_PART(metadata$filename, '/', 3)
   || '/' || SPLIT_PART(metadata$filename, '/', 4)
   || '/' || SPLIT_PART(metadata$filename, '/', 5), 'YYYY/MM/DD'),
 timestamp bigint AS (value:timestamp::bigint),
 col2 varchar AS (value:col2::varchar))
 PARTITION BY (date_part)
 LOCATION=@s1/logs/
 AUTO_REFRESH = true
 FILE_FORMAT = (TYPE = PARQUET)
 AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:001234567890:s3_mybucket';


