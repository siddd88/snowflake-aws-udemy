use role accountadmin;

use "ECOMMERCE_DB"."ECOMMERCE_LIV";

create table orders_test as select * from "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" limit 20;

create or replace masking policy mask_order_value as (val NUMBER) returns number ->
  case
    when current_role() in ('reporting_intern','REPORTING_INTERN') then 99999999999999999999
    else val
  end;

ALTER TABLE IF EXISTS "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS_TEST"
 MODIFY COLUMN O_TOTALPRICE SET MASKING POLICY mask_order_value;