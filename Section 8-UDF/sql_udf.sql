use role sysadmin;

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

-- Simple UDF ----
CREATE or replace FUNCTION sum_values(a number,b number)
  RETURNS number
  LANGUAGE SQL 
  AS
  $$
    SELECT a+b as res
  $$;

select sum_values(2,3);

---Scalar Function to calculate sales quantity by Supplier --- 
CREATE or replace FUNCTION sales_qty_by_supplier(ship_date date,supplier_key integer)
  RETURNS NUMERIC(11,2)
  AS
  $$
    SELECT SUM(l_quantity) as total_quantity_shipped
        FROM "ECOMMERCE_DB"."ECOMMERCE_LIV"."LINEITEM"
        where L_SHIPDATE =ship_date and l_suppkey=supplier_key
  $$;


--- Use the above Scalar function in a query ---- 
select 
    sales_by_supplier('1996-06-27',S_SUPPKEY) as supplier_sales,
    s_suppkey 
from 
    "ECOMMERCE_DB"."ECOMMERCE_LIV"."SUPPLIER"
where supplier_sales>0;



--- Tabular UDF ----
CREATE or replace FUNCTION sales_qty_by_supplier(ship_date varchar,supplier_key number)
  RETURNS table(supplier_key number,qty_sold number)
  AS
  $$
    select 
        lt.L_SUPPKEY ,
        sum(L_QUANTITY) as qty_sold 
    from 
        "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1000"."SUPPLIER" sp 
    join 
        "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF10"."LINEITEM" lt on sp.S_SUPPKEY = lt.L_SUPPKEY
    where lt.l_shipdate=ship_date and lt.L_SUPPKEY=supplier_key
    group by 1
    having qty_sold > 0
  $$;

--   Call the above tabular UDTF ---
select * from table (sales_qty_by_supplier('1996-06-27',23661));