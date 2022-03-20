from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession \
        .builder \
        .appName("Glue-pyspark-test") \
        .getOrCreate()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
snowflake_database="ECOMMERCE_DB"
snowflake_schema="ECOMMERCE_DEV"
source_table_name="LINEITEM"

snowflake_options = {
    "sfUrl":"",
    "sfUser": "",
    "sfPassword": "",
    "sfDatabase": snowflake_database,
    "sfSchema": snowflake_schema,
    "sfWarehouse": "COMPUTE_WH"
}

df = spark.read \
    .format(SNOWFLAKE_SOURCE_NAME) \
    .options(**snowflake_options) \
    .option("dbtable",source_table_name) \
    .option("autopushdown", "on") \
    .load()


df.write.format("snowflake") \
    .options(**snowflake_options) \
    .option("dbtable","spark_output_table").mode("overwrite") \
    .save()


# s3://sf-glue-jobs/jars/snowflake-jdbc-3.13.15.jar,s3://sf-glue-jobs/jars/spark-snowflake_2.12-2.9.2-spark_3.1.jar