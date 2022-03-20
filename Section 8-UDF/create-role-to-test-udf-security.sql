use role accountadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

create role udf_role;

grant usage on warehouse compute_wh to role udf_role;

grant usage on database ECOMMERCE_DB to role udf_role;

grant usage on schema ECOMMERCE_LIV to role udf_role;

grant select on all tables in schema ECOMMERCE_LIV  to role udf_role;

grant all privileges on function get_total_qty_shipped(date,number) to role udf_role;

grant role udf_role to user udf_developer;

alter user udf_developer set default_role=udf_role;


-- Test the GET_DDL using the new user account created above  -----
use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";
select get_ddl('function','get_total_qty_shipped(date,number)');
select get_total_qty_shipped('1997-03-19',4072277) as total_qty_shipped;