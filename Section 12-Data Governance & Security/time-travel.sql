use role sysadmin;

SHOW TABLES;

alter table "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM" set data_retention_time_in_days=90;

ALTER database ECOMMERCE_DB set data_retention_time_in_days=90;

ALTER schema ECOMMERCE_DB.ECOMMERCE_LIV set data_retention_time_in_days=90;

create table lineitem_test as select * from LINEITEM limit 10;

SELECT * FROM LINEITEM before(timestamp => '2022-03-19 02:31:56.786'::timestamp) limit 10;

SELECT * FROM LINEITEM at(timestamp => '2022-03-19 02:31:56.786'::timestamp) limit 10;

CREATE TABLE LINEITEM_restored CLONE LINEITEM at(timestamp => '2022-03-19 02:31:56.786'::timestamp)

create database testing_db;

alter database testing_db set data_retention_time_in_days=0;

drop database testing_db;

undrop database testing_db;