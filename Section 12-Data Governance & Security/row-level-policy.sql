use role accountadmin;

create database test_db;

create schema test_schema;

create or replace table test_db.test_schema.item as 
select * from "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."ITEM" where i_category in ('Sports','Music') limit 100;

CREATE TABLE access_management (role string, category string);

INSERT INTO access_management VALUES ('SPORTS_ADMIN', 'Sports'), ('MUSIC_ADMIN', 'Music');

select * from access_management;

CREATE or replace ROW ACCESS POLICY category_level_access AS
(category_filter VARCHAR) RETURNS BOOLEAN ->
CURRENT_ROLE() = 'ACCOUNTADMIN'
OR EXISTS 
(
  SELECT 1 FROM access_management
  WHERE category = category_filter
  AND role = CURRENT_ROLE()
);


ALTER TABLE item ADD ROW ACCESS POLICY category_level_access ON (i_category);

select * from item;


