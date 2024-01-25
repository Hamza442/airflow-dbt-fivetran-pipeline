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

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

schema = 'sales'
GLUE_DATABASE = "mysql_rds_oltp"
GLUR_STG_DATABASE = 'staging_mysql_rds_oltp'
CATALOG = "glue_catalog"

DEST_BUCKET = "processed-data-bucket-ft"
SOURCE_BUCKET = "raw-data-bucket-ft-new"
EOW_DATE = "9999-12-31"
DATE_FORMAT = "yyyy-MM-dd"
scd2_cols = ["effective_date","expiration_date","current_flag"]
cols_to_drop = ["_fivetran_deleted","_fivetran_synced"]
iceberg_format_version='2'

tbl_stg_address = 'stg_dim_address'
tbl_stg_product = 'stg_dim_product'
tbl_stg_customer = 'stg_dim_customer'
tbl_stg_credit = 'stg_dim_credit_card'
tbl_stg_order_status = 'stg_dim_order_status'

SNOWFLAKE_DB = 'FIVETRAN_DATABASE'
SNOWFLAKE_CONNECTION_NAME = 'snowflake_connection'

def delete_files_in_s3_folder(bucket_name, folder_path):
    
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
    for obj in response.get('Contents', []):
        s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        print(f"Deleted: s3://{bucket_name}/{obj['Key']}")

def column_renamer(df, suffix, append):
    """
    input:
        df: dataframe
        suffix: suffix to be appended to column name
        append: boolean value 
                if true append suffix else remove suffix
    
    output:
        df: df with renamed column
    """
    if append:
        new_column_names = list(map(lambda x: x+suffix, df.columns))
    else:
        new_column_names = list(map(lambda x: x.replace(suffix,""), df.columns))
    return df.toDF(*new_column_names)

def get_hash(df, keys_list):
    """
    input:
        df: dataframe
        key_list: list of columns to be hashed    
    output:
        df: df with hashed column
    """
    columns = [col(column) for column in keys_list]
    if columns:
        return df.withColumn("hash_md5", md5(concat_ws("", *columns)))
    else:
        return df.withColumn("hash_md5", md5(lit(1)))

def read_from_source(SOURCE_TABLE):
    df_current = spark.read.format("iceberg").load(f"{CATALOG}.{GLUE_DATABASE}.{SOURCE_TABLE}")
    return df_current
                
def read_from_destinations(DEST_PATH):
    df = glueContext.create_dynamic_frame.from_options( connection_type = "s3", connection_options = {"paths": [DEST_PATH]}, format = "parquet", transformation_ctx = "datasource0")
    dfnew = df.toDF()
    return dfnew

def insert_changes(PK
                   ,SOURCE_TABLE
                   ,DEST_PATH
                   ,DEST_TABLE_PATH
                   ,window_spec
                   ,type2_cols
                   ,destination_table):
    
    dataFrame = spark.read.format("iceberg").load(f"{CATALOG}.{GLUR_STG_DATABASE}.{SOURCE_TABLE}")
    # df2 = dataFrame.filter(col("_fivetran_deleted") == lit(False))
    df2 = dataFrame
    df_history = read_from_destinations(DEST_PATH)
    
    max_sk = df_history.agg({"sk_id": "max"}).collect()[0][0]

    filtered_df_2 = df2\
                    .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                    .withColumn("expiration_date",date_format(lit(EOW_DATE),DATE_FORMAT))\
                    .withColumn("row_number",row_number().over(window_spec))\
                    .withColumn("sk_id",col("row_number")+ max_sk)\
                    .withColumn("current_flag", lit(True))\
                    .drop("row_number")

    df_history_open = df_history.where(col("current_flag")==lit(True))
    df_history_closed = df_history.where(col("current_flag")==lit(False))
    df_history_open_hash = column_renamer(get_hash(df_history_open, type2_cols), suffix="_history", append=True)
    df_current_hash = column_renamer(get_hash(filtered_df_2, type2_cols), suffix="_current", append=True)
    
    df_merged = df_history_open_hash\
            .join(df_current_hash, col(f"{PK}_current") ==  col(f"{PK}_history"), how="full_outer")\
            .withColumn("Action", when(col("hash_md5_current") == col("hash_md5_history")  , 'NOCHANGE')\
            .when(col(f"{PK}_current").isNull(), 'DELETE')\
            .when(col(f"{PK}_history").isNull(), 'INSERT')\
            .otherwise('UPDATE'))

    df_nochange = column_renamer(df_merged.filter(col("action") == 'NOCHANGE'), suffix="_history", append=False)\
                .select(df_history_open.columns)
                
    df_insert = column_renamer(df_merged.filter(col("action") == 'INSERT'), suffix="_current", append=False)\
                .select(df_history_open.columns)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("expiration_date",date_format(lit(EOW_DATE),DATE_FORMAT))\
                .withColumn("row_number",row_number().over(window_spec))\
                .withColumn("sk_id",col("row_number")+ max_sk)\
                .withColumn("current_flag", lit(True))\
                .drop("row_number")
    
    max_sk = df_insert.agg({"sk_id": "max"}).collect()[0][0]

    df_deleted = column_renamer(df_merged.filter(col("action") == 'DELETE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("current_flag", lit(False))
    
    df_update = column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_history", append=False)\
                .select(df_history_open.columns)\
                .withColumn("expiration_date", date_format(current_date(),DATE_FORMAT))\
                .withColumn("current_flag", lit(False))\
            .unionByName(
            column_renamer(df_merged.filter(col("action") == 'UPDATE'), suffix="_current", append=False)\
                .select(df_history_open.columns)\
                .withColumn("effective_date",date_format(current_date(),DATE_FORMAT))\
                .withColumn("expiration_date",date_format(lit(EOW_DATE),DATE_FORMAT))\
                .withColumn("row_number",row_number().over(window_spec))\
                .withColumn("sk_id",col("row_number")+ max_sk)\
                .withColumn("current_flag", lit(True))\
                .drop("row_number"))
    
    df_final = df_history_closed\
            .unionByName(df_nochange)\
            .unionByName(df_insert)\
            .unionByName(df_deleted)\
            .unionByName(df_update)
    print(df_final.show())

    delete_files_in_s3_folder(DEST_BUCKET,DEST_TABLE_PATH)
    write_to_destination(df_final,DEST_PATH)
    insert_to_snowflake(df_final,schema,destination_table)
    

def write_to_destination(df,DEST_PATH):
    print("write")
    df.write.mode('overwrite').parquet(DEST_PATH)

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

def insert_to_snowflake(df,schema,table):
    
    df_dynamic = DynamicFrame.fromDF(df, glueContext, "test_nest")
    Snowflake_node1705486596887 = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic,
    connection_type="snowflake",
    connection_options={
        "autopushdown": "on",
        "dbtable": f"{table}",
        "connectionName": f"{SNOWFLAKE_CONNECTION_NAME}",
        "preactions": f"TRUNCATE TABLE {schema}.{table};",
        "sfDatabase": f"{SNOWFLAKE_DB}",
        "sfSchema": f"{schema}",
    },
    transformation_ctx="Snowflake_node1705486596887",
    )

    
def main(src_tbl,pk,tp2_cols,destination_table):
    
    # SOURCE_TABLE = "customers"
    # PK = "customerid"
    # type2_cols = ["FirstName", "LastName", "Email", "Phone"]
    SOURCE_TABLE = src_tbl
    PK = pk
    DEST_TABLE_PATH = f"{GLUE_DATABASE}/{SOURCE_TABLE}_dim/"
    SOURCE_PATH = f"s3://{SOURCE_BUCKET}/{GLUE_DATABASE}/{SOURCE_TABLE}/data"
    DEST_PATH = f"s3://{DEST_BUCKET}/{DEST_TABLE_PATH}"
    window_spec  = Window.orderBy(PK)
    type2_cols = tp2_cols
    
    print(f"Table:{SOURCE_TABLE} , PK:{PK}")
    print(f"DEST_TABLE_PATH: {DEST_TABLE_PATH}")
    print(f"SOURCE_PATH: {SOURCE_PATH}")
    print(f"DEST_PATH: {DEST_PATH}")
    print(f"window_spec: {window_spec}")
    print(f"type2_cols: {type2_cols}")


    flag = data_exist(DEST_BUCKET,DEST_TABLE_PATH)
    if not flag:
        print("First time load")
        # df = read_from_source(SOURCE_TABLE)
        df = spark.read.format("iceberg").load(f"{CATALOG}.{GLUR_STG_DATABASE}.{SOURCE_TABLE}")
        print("Done reading")
        print(df.show(5))
        df = df\
            .withColumn("sk_id",row_number().over(window_spec))\
            .withColumn("effective_date",date_format(current_date(), DATE_FORMAT))\
            .withColumn("expiration_date",date_format(lit(EOW_DATE), DATE_FORMAT))\
            .withColumn("current_flag", lit(True))
    
        df_new = df.drop(*cols_to_drop)
        write_to_destination(df_new,DEST_PATH)
        insert_to_snowflake(df_new,schema,destination_table)
    else:
        print("Incremental load")
        insert_changes(PK
                   ,SOURCE_TABLE
                   ,DEST_PATH
                   ,DEST_TABLE_PATH
                   ,window_spec
                   ,type2_cols
                   ,destination_table)


main(tbl_stg_address,'addressid'
    ,["city_name", "state_name", "country_name"]
    ,'dim_address')
main(tbl_stg_credit,'creditcardid'
    ,["cardtype"]
    ,'dim_creditcard')
main(tbl_stg_customer,'customerid'
    ,["businessentityid", "fullname","storebusinessentityid", "storename"]
    ,'dim_customer')
main(tbl_stg_order_status,'order_status'
    ,["order_status_name"]
    ,'dim_orderstatus')
main(tbl_stg_product,'productid'
    ,["product_name","productnumber","color","product_subcategory_name","product_category_name"]
    ,'dim_product')
    


job.commit()