use role sysadmin ; 

--- Change the warehouse name if need be ----
use warehouse prod_xl; 
use database ecommerce_db;
use schema ecommerce_db.ecommerce_liv;

-- View for Orders with "Urgent" Priority ---
create or replace view urgent_priority_orders as 
select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" where o_orderpriority='1-URGENT'


-- Create a materialized view ---
create or replace materialized view vw_aggregated_orders as 
select 
    count(1) as total_orders, 
    O_ORDERSTATUS as order_status , 
    O_ORDERDATE as order_date
from 
    "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS"
where o_orderpriority='1-URGENT'
group by 2,3;


-- Create a secure materialized view ---
create or replace materialized view secure_vw_aggregated_orders as 
select 
    count(1) as total_orders, 
    O_ORDERSTATUS as order_status , 
    O_ORDERDATE as order_date
from 
    "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS"
where o_orderpriority='1-URGENT'
group by 2,3;


---- Add a clustering key to the view 
ALTER MATERIALIZED VIEW vw_aggregated_orders CLUSTER BY (order_date);
ALTER MATERIALIZED VIEW secure_vw_aggregated_orders CLUSTER BY (order_date);

