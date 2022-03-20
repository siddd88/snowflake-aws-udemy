------ Create Role for Column-Level Access Policy - Data Masking --------

create or replace role reporting_intern;
grant usage on warehouse prod_xl to role reporting_intern;
grant usage on database ECOMMERCE_DB to role reporting_intern;
grant usage on schema ECOMMERCE_DB.ECOMMERCE_LIV to role reporting_intern;
grant select on ECOMMERCE_DB.ECOMMERCE_LIV.orders_test to role reporting_intern;
grant role reporting_intern to user siddharth;

---- Switch the role ------- 
use role reporting_intern;
use warehouse prod_xl;
---- Test the column level masking policy -------
select * from ECOMMERCE_DB.ECOMMERCE_LIV.orders_test;



------ Create Role for Row-Level Access Policy --------

use role accountadmin;

create role SPORTS_ADMIN;

grant usage on warehouse prod_xl to role SPORTS_ADMIN;

grant usage on database TEST_DB to role SPORTS_ADMIN;

grant usage on schema TEST_DB.TEST_SCHEMA to role SPORTS_ADMIN;

grant select on TEST_DB.TEST_SCHEMA.ITEM to role SPORTS_ADMIN;

grant role SPORTS_ADMIN to user siddharth;

---- Switch the role ------- 
use role SPORTS_ADMIN;
use warehouse prod_xl;
---- Test the Row Level Access Policy -------
select * from TEST_DB.TEST_SCHEMA.ITEM ;


----- Remove Column Level Masking  -------

use role accountadmin; 
ALTER TABLE orders_test  MODIFY COLUMN O_TOTALPRICE UNSET MASKING POLICY
