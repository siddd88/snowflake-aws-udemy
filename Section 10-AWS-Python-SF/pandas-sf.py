import pandas as pd
import sys
import snowflake.connector
# from awsglue.utils import getResolvedOptions

conn = snowflake.connector.connect(
    user="",
    password="",
    account="",
    warehouse="compute_wh",
    database="ecommerce_db",
    schema="ECOMMERCE_DEV",
    role='SYSADMIN',
    session_parameters={
        'TIMEZONE': 'UTC',
    }
)

try:
    sql = """
        select * from lineitem limit 10
    """
    data_agg = pd.read_sql(sql, conn)
    print(data_agg.head())
finally:
    conn.close()