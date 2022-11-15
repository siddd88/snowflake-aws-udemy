    use role accountadmin; 
     
    CREATE DATABASE SNOWFLAKE_SAMPLE_DATA FROM SHARE SFC_SAMPLES.SAMPLE_DATA;
     
    GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_SAMPLE_DATA TO ROLE sysadmin;
     
    use role sysadmin ; 
     
    create database if not exists ecommerce_db;
     
    create schema if not exists ecommerce_liv;
     
    use schema ecommerce_db.ecommerce_liv;
     
    use warehouse compute_wh;
     
    create or replace table LINEITEM cluster by (L_SHIPDATE) as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."LINEITEM" limit 2000000;
     
    create or replace table ORDERS as select * from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."ORDERS" ;
