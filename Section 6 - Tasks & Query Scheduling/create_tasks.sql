use  ecommerce_db.ecommerce_liv;

 --- Create a table to store the results ----
create or replace TABLE DAILY_AGGREGATED_SUMMARY (
	SUM_QTY NUMBER(24,2),
	TOTAL_BASE_PRICE NUMBER(24,2),
	TOTAL_DISCOUNT_PRICE NUMBER(37,4),
	TOTAL_CHARGE NUMBER(38,6),
	ORDER_COUNT NUMBER(18,0),
	SHIPPED_DATE DATE,
	SHIPPED_MODE VARCHAR(10)
);

create or replace TABLE ORDERS_BY_SHIPMODE (
	TOTAL_ORDERS NUMBER(30,0),
	TOTAL_DISCOUNT NUMBER(38,0),
	SHIPPED_DATE DATE,
	SHIPPED_MODE VARCHAR(10)
);

-- Standalone Task ----- 
create task TSK_DAILY_SALES_SUMMARY
warehouse = prod_xl
schedule = 'using cron 0 8 * * * UTC' as 
insert into "ECOMMERCE_DB"."ECOMMERCE_LIV"."DAILY_AGGREGATED_SUMMARY"
select       
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as total_base_price,
       sum(l_extendedprice * (1-l_discount)) as total_discount_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as total_charge,
       count(*) as order_count,
       date(l_shipdate) as shipped_date,
       l_shipmode as shipped_mode
 from
       "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
 where
       shipped_date = '1996-07-17'
 group by
       shipped_date,
       shipped_mode;

show tasks; 

alter task DAILY_AGGREGATED_SUMMARY resume ; 

-- Dependent Task ----- 

create task TSK_ORDERS_BY_SHIPMODE
warehouse = prod_xl
AFTER TSK_DAILY_SALES_SUMMARY
insert into "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS_BY_SHIPMODE" 
select 
    round(sum(order_count)) as total_orders , 
    round(sum(total_discount_price),0) as total_discount,
    shipped_date,
    shipped_mode
from 
    daily_aggregated_summary
group by 3,4


---- Check Task History ------ 
use role accountadmin;

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'task_name'));



