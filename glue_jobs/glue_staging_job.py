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
schema = 'sales'
GLUE_DATABASE = "mysql_rds_oltp"
GLUR_STG_DATABASE = 'staging_mysql_rds_oltp'
CATALOG = "glue_catalog"
SNOWFLAKE_DB = 'FIVETRAN_DATABASE'
SNOWFLAKE_CONNECTION_NAME = 'snowflake_connection'

cols_to_drop = ["_fivetran_deleted","_fivetran_synced","_fivetran_id","_fivetran_index"]

dim_address_denorm_tbls = ['person_address','person_stateprovince','person_countryregion']
dim_product_denorm_tbls = ['production_product','production_productsubcategory','production_productcategory']
dim_customer_denorm_tbls = ['sales_customer','person_person','sales_store']
dim_credit_denorm_tbls = ['sales_salesorderheader','sales_creditcard']
dim_order_status_denorm_tbls = 'sales_salesorderheader'
dim_date_denorm_tbls = 'date'

tbl_stg_address = 'stg_dim_address'
tbl_stg_product = 'stg_dim_product'
tbl_stg_customer = 'stg_dim_customer'
tbl_stg_credit = 'stg_dim_credit_card'
tbl_stg_order_status = 'stg_dim_order_status'
tbl_stg_date = 'stg_date'


def dim_address_staging_tbl(dim_tables,stg_tbl_name):
    print("============Creating DIM_ADDRESS============")
    print(dim_tables,stg_tbl_name)
    
    tlb_address, tbl_stateprovince, tbl_countryregion = dim_tables
    
    address = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tlb_address}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    stateprovince = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_stateprovince}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    countryregion = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_countryregion}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    dim_address = address.join(stateprovince
                              ,address.stateprovinceid == stateprovince.stateprovinceid
                              ,'left')\
                              .join(countryregion
                              ,stateprovince.countryregioncode == countryregion.countryregioncode
                              ,'left')\
                              .select(address.addressid
                              ,address.city.alias("city_name")
                              ,stateprovince.name.alias("state_name")
                              ,countryregion.name.alias("country_name"))
    
    print(dim_address.show(5))
    
    dim_address.writeTo(f'{CATALOG}.{GLUR_STG_DATABASE}.{stg_tbl_name}')\
    .tableProperty('format-version', iceberg_format_version)\
    .createOrReplace()
 
  
def dim_credit_staging_tbl(dim_tables,stg_tbl_name):
    print("============Creating DIM_CREDITCARD============")
    print(dim_tables,stg_tbl_name)
    
    tbl_salesorderheader,tbl_creditcard = dim_tables

    stg_salesorderheader = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_salesorderheader}")\
    .filter("creditcardid IS NOT NULL")\
    .filter(col("_fivetran_deleted") == lit(False))\
    .select("creditcardid").distinct()
    
    stg_creditcard = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_creditcard}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    dim_credit_card = stg_salesorderheader\
    .join(stg_creditcard,stg_salesorderheader.creditcardid == stg_creditcard.creditcardid,'left')\
    .select(stg_salesorderheader.creditcardid,stg_creditcard.cardtype)
    
    print(dim_credit_card.show(5))
    
    dim_credit_card.writeTo(f'{CATALOG}.{GLUR_STG_DATABASE}.{stg_tbl_name}')\
    .tableProperty('format-version', iceberg_format_version)\
    .createOrReplace()


def dim_customer_staging_tbl(dim_tables,stg_tbl_name):
    print("============Creating DIM_CUSTOMER============")
    print(dim_tables,stg_tbl_name)
    
    tbl_customer,tbl_person,tbl_store = dim_tables
    
    stg_customer = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_customer}")\
    .filter(col("_fivetran_deleted") == lit(False))\
    .select("customerid","personid","storeid")
    
    stg_person_new = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_person}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    stg_person = stg_person_new.withColumn("fullname", concat_ws(" ", "firstname", "middlename", "lastname"))\
    .select("businessentityid","fullname")

    stg_store = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_store}")\
    .filter(col("_fivetran_deleted") == lit(False))\
    .select(col("businessentityid")\
    .alias("storebusinessentityid"),"name")
    
    dim_customer = stg_customer.join(stg_person,stg_customer.personid == stg_person.businessentityid,'left')\
    .join(stg_store,stg_customer.storeid == stg_store.storebusinessentityid,'left')\
    .select(stg_customer.customerid
    ,stg_person.businessentityid
    ,stg_person.fullname
    ,stg_store.storebusinessentityid
    ,stg_store.name.alias("storename"))
    
    print(dim_customer.show(5))
    
    dim_customer.writeTo(f'{CATALOG}.{GLUR_STG_DATABASE}.{stg_tbl_name}')\
    .tableProperty('format-version', iceberg_format_version)\
    .createOrReplace()


def dim_order_status_staging_tbl(dim_tables,stg_tbl_name):
    print("============Creating DIM_ORDERSTATUS============")
    print(dim_tables,stg_tbl_name)
    tbl_salesorderheader = dim_tables

    stg_order_status = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_salesorderheader}")\
    .filter(col("_fivetran_deleted") == lit(False))\
    .select(col("status").alias("order_status")).distinct()
    
    result_df = stg_order_status.withColumn(
    "order_status_name",
    when(col("order_status") == 1, "in_process")
    .when(col("order_status") == 2, "approved")
    .when(col("order_status") == 3, "backordered")
    .when(col("order_status") == 4, "rejected")
    .when(col("order_status") == 5, "shipped")
    .when(col("order_status") == 6, "cancelled")
    .otherwise("no_status")
    ).select("order_status", "order_status_name")
    
    print(result_df.show(5))

    result_df.writeTo(f'{CATALOG}.{GLUR_STG_DATABASE}.{stg_tbl_name}')\
    .tableProperty('format-version', iceberg_format_version)\
    .createOrReplace()


def dim_product_staging_tbl(dim_tables,stg_tbl_name):
    print("============Creating DIM_PRODUCT============")
    print(dim_tables,stg_tbl_name)
    tbl_product,tbl_productsubcategory,tbl_productcategory = dim_tables

    stg_product = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_product}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    stg_product_subcategory = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_productsubcategory}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    stg_product_category = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{tbl_productcategory}")\
    .filter(col("_fivetran_deleted") == lit(False))
    
    dim_product = stg_product.join(stg_product_subcategory
                                   ,stg_product.productsubcategoryid == stg_product_subcategory.productsubcategoryid
                                   ,'left'
                             ).join(stg_product_category
                                    ,stg_product_subcategory.productcategoryid == stg_product_category.productcategoryid
                                    ,'left'
                             ).select(stg_product.productid
                                     ,stg_product.name.alias("product_name")
                                     ,stg_product.productnumber
                                     ,stg_product.color
                                    #  ,stg_product.class
                                     ,stg_product_subcategory.name.alias("product_subcategory_name")
                                     ,stg_product_category.name.alias("product_category_name"))
    
    print(dim_product.show(5))
                                 
    dim_product.writeTo(f'{CATALOG}.{GLUR_STG_DATABASE}.{stg_tbl_name}')\
    .tableProperty('format-version', iceberg_format_version)\
    .createOrReplace()


def dim_date_tbl(dim_date_tbl,stg_tbl_name):
    print("============Creating DIM_DATE============")
    print(dim_date_tbl,stg_tbl_name)
    
    date = spark.read.format("iceberg")\
    .load(f"{CATALOG}.{GLUE_DATABASE}.{dim_date_tbl}").filter(col("_fivetran_deleted") == lit(False))\
    .drop(*cols_to_drop)
    
    print(date.show(5))
    
    insert_to_snowflake(date,schema,stg_tbl_name)

    
def insert_to_snowflake(df,schema,table):
    print(f"Inserting table: {table}")
    
    df_dynamic = DynamicFrame.fromDF(df, glueContext, "test_nest")

    Snowflake_node1705486596887 = glueContext.write_dynamic_frame.from_options(
    frame=df_dynamic,
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

def create_staging_tables():
    
    dim_address_staging_tbl(dim_address_denorm_tbls,tbl_stg_address)
    dim_product_staging_tbl(dim_product_denorm_tbls,tbl_stg_product)
    dim_customer_staging_tbl(dim_customer_denorm_tbls,tbl_stg_customer)
    dim_credit_staging_tbl(dim_credit_denorm_tbls,tbl_stg_credit)
    dim_order_status_staging_tbl(dim_order_status_denorm_tbls,tbl_stg_order_status)
    dim_date_tbl(dim_date_denorm_tbls,tbl_stg_date)

create_staging_tables()    


job.commit()