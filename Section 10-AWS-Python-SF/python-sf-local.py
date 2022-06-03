import snowflake.connector

ctx = snowflake.connector.connect(
    user="konhee93",
    password="dlrjsgml94SN!",
    account="gi41171.ca-central-1.aws",
    warehouse="compute_wh",
    database="ecommerce_db",
    schema="ECOMMERCE_DEV",
    role='SYSADMIN',
    session_parameters={
        'TIMEZONE': 'UTC',
    }
)

cs = ctx.cursor()
try:
    sql = """select * from LINEITEM  limit 10"""
    cs.execute(sql)
    # one_row = cs.fetchone()
    all_rows = cs.fetchall()
    # print(one_row[0])
    print(all_rows)

finally:
    cs.close()
ctx.close()