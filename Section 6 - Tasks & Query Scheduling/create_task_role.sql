use role accountadmin;

create role task_owner;

grant usage on warehouse prod_xl to role task_owner;

grant usage on database ECOMMERCE_DB to role task_owner;

grant usage on schema ECOMMERCE_DB.ECOMMERCE_LIV to role task_owner;

grant select on "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" to role task_owner;
grant select on "ECOMMERCE_DB"."ECOMMERCE_LIV"."ORDERS" to role task_owner;

grant create task on schema ECOMMERCE_DB.ECOMMERCE_LIV to role task_owner;
grant execute task on account to role task_owner;
GRANT OWNERSHIP ON all tasks in schema "ECOMMERCE_DB"."ECOMMERCE_LIV" TO ROLE task_owner;
grant create table on schema ECOMMERCE_DB.ECOMMERCE_LIV to role task_owner;

grant role task_owner to user siddharth;

use role task_owner;