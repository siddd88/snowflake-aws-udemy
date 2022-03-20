
create or replace stage "ECOMMERCE_DB"."ECOMMERCE_DEV"."stg_orders_csv_dev"
storage_integration = aws_sf_data
url = '{bucket_name}'
file_format = csv_load_format;

copy into "ECOMMERCE_DB"."ECOMMERCE_DEV"."ORDERS" from @stg_orders_csv_dev ON_ERROR = ABORT_STATEMENT;

