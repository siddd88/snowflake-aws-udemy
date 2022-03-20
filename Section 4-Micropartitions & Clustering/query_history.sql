
--  Retreive history of all the queries  ----
select * from table(information_schema.query_history()) order by start_time;

select * from table(information_schema.query_history_by_user()) order by start_time;

select * from table(information_schema.query_history_by_warehouse()) order by start_time;


WITH access_history AS (
   SELECT distinct query_id "QUERY_ID"
     ,split(base.value:objectName, '.')[0]::string "DATABASE_NAME"
     ,split(base.value:objectName, '.')[1]::string "SCHEMA_NAME"
     ,split(base.value:objectName, '.')[2]::string "TABLE_NAME"
FROM snowflake.account_usage.access_history
     ,lateral flatten (base_objects_accessed) base
)
select regexp_replace(replace(replace(query_text,char(10),' '),char(13),' '),'  *',' ') query_text
    ,count(*) query_count
    ,(avg(execution_time) / 1000) / 60 avg_exec_time_mins
    ,avg((nullifzero(PARTITIONS_SCANNED) / nullifzero(PARTITIONS_TOTAL))) * 100 "avg_%_paritions_scanned"
from snowflake.account_usage.query_history qh
 join access_history ah
on qh.query_id = ah.query_id
where to_date(start_time) >= dateadd('months', -1, current_date())
//and ah.database_name = 'snowflake_sample_data'
//and ah.table_name = <TABLE_NAME>
and error_code is null
//and (query_text ilike '%where%' or query_text ilike '%join%')
and warehouse_size is not null
group by 1
//having avg_exec_time_mins > 0.1
order by query_count desc;

