from pyspark.sql.types import IntegerType
from commonFunctions.helpers import create_spark_session
from pyspark.sql import functions as F
import configparser

def fullLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps
    1. Join SatingOrders with Staging products to get the department
    2. Read Aisles, Dim Product and Department Dim to get the keys
    3. Join StagingFactOrderItems with Aisles and Department Dims
    4. Filling not match records whit -1 Key (Undefined)
    5. Write results in S3
    """
    # Reading Datasets
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")
    dimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")
    dimDepartments = spark.read.parquet(s3 + "/presentation_layer/dim_department")
    dimProduct=spark.read.parquet(s3+"/presentation_layer/dim_product")\
    .withColumnRenamed("aisle_key","product_aisle_key")

    StagingFactOrderItems=stagingOrders.join(stagingProducts,stagingOrders["PRODUCT"]==stagingProducts["product_name"],"left")\
        .drop("product_name","aisle","ADD_TO_CART_ORDER")\

    StagingFactOrderswAisle = StagingFactOrderItems.join(dimAisle,
                                                         StagingFactOrderItems["AISLES"] == dimAisle["aisle_name"],
                                                         "left") \
        .drop("AISLES", "aisle_name", "inserted_date")

    StagingFactOrderswAislewProduct=StagingFactOrderswAisle.join(dimProduct,
                                                                 [StagingFactOrderswAisle["PRODUCT"] == dimProduct["product_name"],
                                                                  StagingFactOrderswAisle["aisle_key"] == dimProduct["product_aisle_key"]],
                                                                 "left")\
        .drop("PRODUCT","product_name","inserted_date","product_aisle_key")

    newFact = StagingFactOrderswAislewProduct.join(dimDepartments,
                                           StagingFactOrderswAislewProduct["department"] == dimDepartments["department_name"],
                                           "left") \
        .drop("department_name", "inserted_date", "department") \
        .withColumnRenamed("ORDER_ID", "order_key") \
        .withColumnRenamed("USER_ID", "user_key") \
        .withColumnRenamed("ORDER_NUMBER", "order_number") \
        .withColumn("day_key",F.when(F.col("ORDER_DOW")=='Sunday',0)\
                    .when(F.col("ORDER_DOW")=='Monday',1)\
                    .when(F.col("ORDER_DOW") == 'Tuesday', 2)\
                    .when(F.col("ORDER_DOW") == 'Wednesday', 3)\
                    .when(F.col("ORDER_DOW") == 'Thursday', 4)\
                    .when(F.col("ORDER_DOW") == 'Friday', 5)\
                    .when(F.col("ORDER_DOW") == 'Saturday', 6)\
                    .otherwise(F.col("ORDER_DOW"))) \
        .drop("ORDER_DOW") \
        .withColumn("hour_key", F.when(F.col("ORDER_HOUR_OF_DAY") == '24', '00') \
                    .otherwise(F.col("ORDER_HOUR_OF_DAY"))) \
        .drop("ORDER_HOUR_OF_DAY") \
        .fillna({'department_key': '-1'}) \
        .fillna({'aisle_key': '-1'}) \
        .fillna({'product_key': '-1'}) \
        .select("order_key","user_key","order_number","day_key","hour_key","aisle_key","department_key","product_key")

    #newFact.show(10,False)
    newFact.write.parquet(s3 + "/presentation_layer/fact_order_items", mode="overwrite")


def incrementalLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    0. Read Current Fact Table from S3
    1. AntiJoin to get the new orders based on the order Id.
    2. Join SatingOrders with Staging products to get the department
    3. Read Aisles and Department Dim to get the keys
    4. Join StagingFactOrderItems with Aisles and Department Dims
    5. Filling not match records whit -1 Key (Undefined)
    6. Write results in S3
    """
    #Reading Datasets
    currentFact=spark.read.parquet(s3+"/presentation_layer/fact_order_items")
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")
    dimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")
    dimDepartments = spark.read.parquet(s3 + "/presentation_layer/dim_department")
    dimProduct=spark.read.parquet(s3+"/presentation_layer/dim_product")\
        .withColumnRenamed("aisle_key","product_aisle_key")

    newOrders = stagingOrders.join(currentFact, stagingOrders["ORDER_ID"] == currentFact["order_key"], "leftanti")

    StagingFactOrderItems = newOrders.join(stagingProducts, newOrders["PRODUCT"] == stagingProducts["product_name"],
                                           "left") \
        .drop("product_name", "aisle", "ADD_TO_CART_ORDER")

    StagingFactOrderswAisle = StagingFactOrderItems.join(dimAisle,
                                                         StagingFactOrderItems["AISLES"] == dimAisle["aisle_name"],
                                                         "left") \
        .drop("AISLES", "aisle_name", "inserted_date")

    StagingFactOrderswAislewProduct=StagingFactOrderswAisle.join(dimProduct,
                                                                 [StagingFactOrderswAisle["PRODUCT"] == dimProduct["product_name"],
                                                                  StagingFactOrderswAisle["aisle_key"] == dimProduct["product_aisle_key"]],
                                                                 "left")\
        .drop("PRODUCT","product_name","inserted_date","product_aisle_key")

    newFact = StagingFactOrderswAislewProduct.join(dimDepartments,
                                           StagingFactOrderswAislewProduct["department"] == dimDepartments["department_name"],
                                           "left") \
        .drop("department_name", "inserted_date", "department") \
        .withColumnRenamed("ORDER_ID", "order_key") \
        .withColumnRenamed("USER_ID", "user_key") \
        .withColumnRenamed("ORDER_NUMBER", "order_number") \
        .withColumn("day_key",F.when(F.col("ORDER_DOW")=='Sunday',0)\
                    .when(F.col("ORDER_DOW")=='Monday',1)\
                    .when(F.col("ORDER_DOW") == 'Tuesday', 2)\
                    .when(F.col("ORDER_DOW") == 'Wednesday', 3)\
                    .when(F.col("ORDER_DOW") == 'Thursday', 4)\
                    .when(F.col("ORDER_DOW") == 'Friday', 5)\
                    .when(F.col("ORDER_DOW") == 'Saturday', 6)\
                    .otherwise(F.col("ORDER_DOW")))\
        .drop("ORDER_DOW") \
        .withColumn("hour_key", F.when(F.col("ORDER_HOUR_OF_DAY") == '24', '00')\
                    .otherwise(F.col("ORDER_HOUR_OF_DAY")))\
        .drop("ORDER_HOUR_OF_DAY") \
        .fillna({'department_key': '-1'}) \
        .fillna({'aisle_key': '-1'}).union(currentFact) \
        .fillna({'product_key': '-1'}) \
        .select("order_key", "user_key", "order_number", "day_key", "hour_key", "aisle_key", "department_key","product_key")

    newFact.write.parquet(s3 + "/tmp/fact_order_items_tmp", mode="overwrite")
    spark.read.parquet(s3 + "/tmp/fact_order_items_tmp") \
        .write.parquet(s3 + "/presentation_layer/fact_order_items", mode="overwrite")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('../dl.cfg')
    loadType = config.get('LOAD', 'LOADTYPE')

    s3 = config.get('S3', 'BUCKET')

    # Getting Spark Session
    spark = create_spark_session()

    if loadType=="full":
        fullLoad(s3,spark)
    else:
        incrementalLoad(s3,spark)