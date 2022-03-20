
import snowflake.connector
from awsglue.utils import getResolvedOptions

con = snowflake.connector.connect(
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

cursor = con.cursor()

sql_query = """
            select * from lineitem limit 100
            """

try : 
    cursor.execute(sql_query)
    query_id = cursor.sfqid
    cursor.get_results_from_sfqid(query_id)
    results = cursor.fetchall()
    print(f'{results[0]}')
    
except Exception as e:
    con.rollback()
    raise e
    
finally:
    con.close() 