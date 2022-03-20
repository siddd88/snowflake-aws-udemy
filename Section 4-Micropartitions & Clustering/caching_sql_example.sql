use role sysadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";


-- Metadata caching ----
select count(1) from lineitem ; 

select min(L_ORDERKEY),max(L_ORDERKEY) from lineitem;


--  Results Data caching : Example Query ----

set current_dt='1998-08-01';

select       
       sum(l_quantity) as sum_qty,
       count(*) as order_count,
       date(l_shipdate) as shipped_date,
       l_shipmode as shipped_mode
 from
       LINEITEM
 where
       shipped_date >= date($current_dt) - 7
 group by
       shipped_date,
       shipped_mode
order by 
    shipped_date;


-- Clear Result Cache ---
ALTER SESSION SET USE_CACHED_RESULT = FALSE;


-- Clear Data Cache ---

alter warehouse {warehouse_name} suspend;

alter warehouse {warehouse_name} resume;