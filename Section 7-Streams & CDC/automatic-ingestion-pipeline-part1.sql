use role accountadmin;

CREATE or REPLACE STORAGE INTEGRATION aws_sf_data
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '{your_iam_arn}'
  STORAGE_ALLOWED_LOCATIONS = ('{your_bucket_name}');

grant usage on integration aws_sf_data to role sysadmin;

grant execute task on account to role sysadmin;

grant create stage on schema "ECOMMERCE_DB"."ECOMMERCE_DEV" to role sysadmin;

use role sysadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_DEV";

desc INTEGRATION aws_sf_data;

CREATE OR REPLACE FILE FORMAT json_load_format TYPE = 'JSON' ;

create or replace stage stg_lineitem_json_dev
storage_integration = aws_sf_data
url = '{your_bucket_name}/streams_dev/'
file_format = json_load_format;

list @stg_lineitem_json_dev;

create or replace table lineitem_raw_json (src variant );

CREATE OR REPLACE STREAM lineitem_std_stream ON TABLE lineitem_raw_json;


create or replace task lineitem_load_tsk 
warehouse = compute_wh
schedule = '1 minute'
when system$stream_has_data('lineitem_std_stream')
as 
merge into lineitem as li 
using 
(
   select 
        SRC:L_ORDERKEY as L_ORDERKEY,
        SRC:L_PARTKEY as L_PARTKEY,
        SRC:L_SUPPKEY as L_SUPPKEY,
        SRC:L_LINENUMBER as L_LINENUMBER,
        SRC:L_QUANTITY as L_QUANTITY,
        SRC:L_EXTENDEDPRICE as L_EXTENDEDPRICE,
        SRC:L_DISCOUNT as L_DISCOUNT,
        SRC:L_TAX as L_TAX,
        SRC:L_RETURNFLAG as L_RETURNFLAG,
        SRC:L_LINESTATUS as L_LINESTATUS,
        SRC:L_SHIPDATE as L_SHIPDATE,
        SRC:L_COMMITDATE as L_COMMITDATE,
        SRC:L_RECEIPTDATE as L_RECEIPTDATE,
        SRC:L_SHIPINSTRUCT as L_SHIPINSTRUCT,
        SRC:L_SHIPMODE as L_SHIPMODE,
        SRC:L_COMMENT as L_COMMENT
    from 
        lineitem_std_stream
    where metadata$action='INSERT'
) as li_stg
on li.L_ORDERKEY = li_stg.L_ORDERKEY and li.L_PARTKEY = li_stg.L_PARTKEY and li.L_SUPPKEY = li_stg.L_SUPPKEY
when matched then update 
set 
    li.L_PARTKEY = li_stg.L_PARTKEY,
    li.L_SUPPKEY = li_stg.L_SUPPKEY,
    li.L_LINENUMBER = li_stg.L_LINENUMBER,
    li.L_QUANTITY = li_stg.L_QUANTITY,
    li.L_EXTENDEDPRICE = li_stg.L_EXTENDEDPRICE,
    li.L_DISCOUNT = li_stg.L_DISCOUNT,
    li.L_TAX = li_stg.L_TAX,
    li.L_RETURNFLAG = li_stg.L_RETURNFLAG,
    li.L_LINESTATUS = li_stg.L_LINESTATUS,
    li.L_SHIPDATE = li_stg.L_SHIPDATE,
    li.L_COMMITDATE = li_stg.L_COMMITDATE,
    li.L_RECEIPTDATE = li_stg.L_RECEIPTDATE,
    li.L_SHIPINSTRUCT = li_stg.L_SHIPINSTRUCT,
    li.L_SHIPMODE = li_stg.L_SHIPMODE,
    li.L_COMMENT = li_stg.L_COMMENT
when not matched then insert 
(
    L_ORDERKEY,
    L_PARTKEY,
    L_SUPPKEY,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE,
    L_COMMITDATE,
    L_RECEIPTDATE,
    L_SHIPINSTRUCT,
    L_SHIPMODE,
    L_COMMENT
) 
values 
(
    li_stg.L_ORDERKEY,
    li_stg.L_PARTKEY,
    li_stg.L_SUPPKEY,
    li_stg.L_LINENUMBER,
    li_stg.L_QUANTITY,
    li_stg.L_EXTENDEDPRICE,
    li_stg.L_DISCOUNT,
    li_stg.L_TAX,
    li_stg.L_RETURNFLAG,
    li_stg.L_LINESTATUS,
    li_stg.L_SHIPDATE,
    li_stg.L_COMMITDATE,
    li_stg.L_RECEIPTDATE,
    li_stg.L_SHIPINSTRUCT,
    li_stg.L_SHIPMODE,
    li_stg.L_COMMENT
);

show tasks;

alter task lineitem_load_tsk resume;

copy into lineitem_raw_json from @stg_lineitem_json_dev ON_ERROR = ABORT_STATEMENT;

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 100));
