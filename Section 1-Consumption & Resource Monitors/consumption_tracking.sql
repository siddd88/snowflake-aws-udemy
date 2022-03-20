
use role accountadmin;

select 
    round(sum(credits_used*3.28),2) as billed_amount,
    warehouse_name,
    TIMESTAMPDIFF('minute',start_time,end_time) as warehouse_run_time_minutes,
    date(start_time) as execution_date,
    hour(start_time) as execution_hour
from snowflake.account_usage.warehouse_metering_history
group by 2,3,4,5


select 
  count(distinct query_id) as total_queries_executed,
  case 
      when round(execution_time/6000) between 0 and 10 then '0-10 minutes'
      when round(execution_time/6000) between 10 and 20 then '10-20 minutes'
      when round(execution_time/6000) between 20 and 50 then '20-50 minutes'
      when round(execution_time/6000) between 50 and 100 then '50-100 minutes'
      when round(execution_time/6000) between 100 and 200 then '100-200 minutes'
      when round(execution_time/6000) between 100 and 200 then '200-300 minutes'
      when round(execution_time/6000) >300 then '300+ minutes'
  end as execution_time_range,
  nvl(warehouse_name,'Warehouse Cache') as warehouse_name,
  user_name,
  date(start_time) as execution_date,
  hour(start_time) as execution_hour
from 
    snowflake.account_usage.query_history
group by 2,3,4,5,6