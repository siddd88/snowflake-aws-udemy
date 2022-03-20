import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

client = boto3.client('glue',region_name='eu-central-1')

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_automation_dag", default_args=args, schedule_interval=None
)

snowflake_query = [
        """
            use role sysadmin;
        """,
        """
            use schema "ECOMMERCE_DB"."ECOMMERCE_LIV";
        """,
                    """copy into lineitem
                    from @stg_lineitem_csv_dev 
                    file_format = csv_load_format 
                    ON_ERROR = ABORT_STATEMENT;
                  """
]

def execute_job(**kwargs) :
    status = client.start_job_run(JobName = "pyspark_sales_data_agg")
    logging.info("GLUE Job Status: %s  Execution Time: %s",status['JobRun']['JobRunState'],status['JobRun']['ExecutionTime'])

    while True:
        try:
            status = client.get_job_run(JobName=kwargs['glueJobName'], RunId=status['JobRunId'])
            if status['JobRun']['JobRunState'] == 'SUCCEEDED':
                break

        except ClientError as e:
            logging.info(e)

with dag:

    copy_data = SnowflakeOperator(
        task_id="insert_snowflake_data",
        sql=snowflake_query ,
        snowflake_conn_id="snowflake_conn"
    )

    glue_task=PythonOperator(task_id="trigger_pyspark",python_callable=execute_job,execution_timeout=timedelta(minutes=15))


copy_data >> glue_task