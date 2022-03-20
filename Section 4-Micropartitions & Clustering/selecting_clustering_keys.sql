use role sysadmin;

set current_dt='1998-08-01';

use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";

with part_type as (
      select p_partkey,p_type as part_type from part
)
select       
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as total_base_price,
       sum(l_extendedprice * (1-l_discount)) as total_discount_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as total_charge,
       count(*) as order_count,
       date(l_shipdate) as shipped_date,
       pt.part_type
 from
       LINEITEM li
 JOIN 
      part_type pt 
 ON
      li.l_partkey = pt.p_partkey
 where
       shipped_date >= date($current_dt) - 30
 group by
       shipped_date,
       pt.part_type;

with part_type as (
    select 
    p_partkey as part_key,
    p_type as part_type 
    from part
),
line_items as (
      select       
       sum(l_quantity) as sum_qty,
       sum(l_extendedprice) as total_base_price,
       sum(l_extendedprice * (1-l_discount)) as total_discount_price,
       sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as total_charge,
       count(*) as order_count,
       l_partkey as part_key,
       date(l_shipdate) as shipped_date
      from 
      LINEITEM
      where 
            shipped_date >= date($current_dt) - 30
      group by 
            shipped_date,part_key
)