CREATE OR REPLACE PROCEDURE df_transform_sql()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python','pandas')
HANDLER = 'df_transform'
AS
$$
def df_transform(session):
    import snowflake.snowpark
    df_orders = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS") \
                .select(['O_ORDERKEY','O_CUSTKEY','O_TOTALPRICE']) \
                .filter("O_TOTALPRICE>=100000")

    df_lineitem = session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM") \
                .select(['L_SUPPKEY','L_ORDERKEY','L_QUANTITY','L_PARTKEY'])

    df_order_lineitem = df_orders.join(df_lineitem,df_lineitem.L_ORDERKEY == df_orders.O_ORDERKEY) \
            .select(['L_ORDERKEY','O_CUSTKEY','O_TOTALPRICE','L_QUANTITY','L_SUPPKEY','L_PARTKEY'])
    
    try : 
        df_order_lineitem.write.mode("overwrite").save_as_table("order_lineitems")
        return 'success'
    except Exception as e : 
        return e
$$;≈