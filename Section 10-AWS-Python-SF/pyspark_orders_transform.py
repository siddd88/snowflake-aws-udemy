from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import count, avg,sum

spark = SparkSession \
        .builder \
        .appName("Glue-pyspark-test") \
        .getOrCreate()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
snowflake_database="ECOMMERCE_DB"
snowflake_schema="ECOMMERCE_LIV"
source_table_name="ORDERS"

snowflake_options = {
    "sfUrl":"",
    "sfUser": "",
    "sfPassword": "",
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": "prod_xl"
}

# df = spark.read \
#     .format(SNOWFLAKE_SOURCE_NAME) \
#     .options(**snowflake_options) \
#     .option("dbtable",source_table_name) \
#     .option("autopushdown", "on") \
#     .load()


fetch_orderes_sql = """
    select 
    O_ORDERKEY,
    O_CUSTKEY,
    O_ORDERSTATUS,
    O_TOTALPRICE,
    O_ORDERDATE 
    from orders 
"""

fetch_lineitems_sql = """
    select 
        L_ORDERKEY,
        L_SHIPDATE,
        L_SHIPMODE
    from LINEITEM 
    where L_SHIPDATE = '1996-02-13'
"""

df_orders = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_options) \
    .option("query",fetch_orderes_sql) \
    .option("autopushdown", "on") \
    .load()

df_lineitems = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_options) \
    .option("query",fetch_lineitems_sql) \
    .option("autopushdown", "on") \
    .load()

df = df_lineitems.join(df_orders,df_orders.O_ORDERKEY==df_lineitems.L_ORDERKEY,"inner")

df_agg = df.groupBy("L_SHIPMODE","L_SHIPDATE").agg(sum("O_TOTALPRICE"), count("O_ORDERKEY"))


df_agg.write.format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable","aggregated_daily_sales").mode("overwrite") \
    .save()


#s3://sf-glue-jobs/spark-jars/snowflake-jdbc-3.13.15.jar,s3://sf-glue-jobs/spark-jars/spark-snowflake_2.12-2.9.2-spark_3.1.jar