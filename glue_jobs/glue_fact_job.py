import sys
import boto3
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

iceberg_format_version='2'
date_col = 'modifieddate' # date column to get incremental data
schema = 's3' # Snowflake schema

CATALOG = 'glue_catalog'
GLUE_DATABASE = 'mysql_rds_oltp'
DEST_BUCKET = 'processed-data-bucket-ft'
SNOWFLAKE_DB = 'FIVETRAN_DATABASE'
SNOWFLAKE_CONNECTION_NAME = 'snowflake_connection'

# Tables from AWS RDS
FACT_SALESDETAIL = 'sales_salesorderdetail'
FACT_SALESHEADER = 'sales_salesorderheader'
# Tables that will be created in Snowflake
FACT_SALESDETAIL_SNOWFLAKE_TBL = 'salesorderdetail'
FACT_SALESHEADER_SNOWFLAKE_TBL = 'salesorderheader'

def data_exist(bucket_name,folder_path):
    flag = None
    s3 = boto3.client('s3')
    objects = s3.list_objects(Bucket=bucket_name, Prefix=folder_path)
    if 'Contents' in objects:
        print(f"The folder '{folder_path}' contains files.")
        flag = True
    else:
        print(f"The folder '{folder_path}' is empty.")
        flag = False
    return flag

def first_load(SOURCE_TABLE,DEST_PATH,destination_table):
    print("====== First Load ======")
    df_source = spark.read.format("iceberg").load(f"{CATALOG}.{GLUE_DATABASE}.{SOURCE_TABLE}").filter(col("_fivetran_deleted") == lit(False))
    df_source.write.mode('overwrite').parquet(DEST_PATH)
    print("====== Loading to Snowflake ======")
    insert_to_snowflake(df_source,schema,destination_table)

def incremental_load(SOURCE_TABLE,DEST_PATH,destination_table):
    print("====== Incremental Load ======")
    date = spark.read.parquet(DEST_PATH).filter(col("_fivetran_deleted") == lit(False)).select(max(col(date_col)).alias("max_date")).collect()
    max_date=[row.max_date for row in date]
    print("Max Date:",max_date[0])
    
    df_new = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{SOURCE_TABLE}")\
    .filter(col(date_col)>max_date[0])
    
    print(df_new.show(5))
    if df_new.rdd.isEmpty():
        print("No new data")
    else:
        df_new.write.mode('overwrite').parquet(DEST_PATH)
        insert_to_snowflake(df_new,schema,destination_table)
    
def insert_to_snowflake(df,schema,table):
    
    df_dynamic = DynamicFrame.fromDF(df, glueContext, "test_nest")
    df_dynamic_new = DropFields.apply(
    frame=df_dynamic,
    paths=["_fivetran_deleted", "_fivetran_synced"],
    transformation_ctx="DropFields_node1705487105509",
    )
    Snowflake_node1705486596887 = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic_new,
    connection_type="snowflake",
    connection_options={
        "autopushdown": "on",
        "dbtable": f"{table}",
        "connectionName": f"{SNOWFLAKE_CONNECTION_NAME}",
        "sfDatabase": f"{SNOWFLAKE_DB}",
        "sfSchema": f"{schema}",
    },
    transformation_ctx="Snowflake_node1705486596887",
    )

def main(SOURCE_TABLE,destination_table):
    
    DEST_TABLE_PATH = f'{GLUE_DATABASE}/fact_{destination_table}'
    DEST_PATH = f's3://{DEST_BUCKET}/{DEST_TABLE_PATH}'
    
    print(f"source: {SOURCE_TABLE} === destination: {destination_table}")
    print(DEST_TABLE_PATH)
    print(DEST_PATH)

    flag = data_exist(DEST_BUCKET,DEST_TABLE_PATH)
    if not flag:
        print("First time load")
        first_load(SOURCE_TABLE,DEST_PATH,destination_table)
    else:
        print("Incremental time load")
        incremental_load(SOURCE_TABLE,DEST_PATH,destination_table)
        
main(FACT_SALESDETAIL,FACT_SALESDETAIL_SNOWFLAKE_TBL)
main(FACT_SALESHEADER,FACT_SALESHEADER_SNOWFLAKE_TBL)
    


job.commit()