from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import *
import configparser


def fullLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps Initial Load:\n
    1. Read Staging Orders, Product Dimension and Aisle Dimension
    2. Join staging Orders with Dim_Aisle to get Aisle Key
    3. Join Resulting DataFrame with Dim Products based on product_name and
    4. Fill not matching rows
    5. Write results on S3
    """

    # Reading Datasets
    dimProduct=spark.read.parquet(s3+"/presentation_layer/dim_product")
    dimAisle=spark.read.parquet(s3+"/presentation_layer/dim_aisle")
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")

    stagingOrderswithAisle = stagingOrders.join(dimAisle, stagingOrders["AISLES"] == dimAisle["aisle_name"],
                                                "leftouter") \
        .withColumnRenamed("aisle_key", "another_aisle_key") \
        .drop("USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "inserted_date")

    dimOrderProductBridge = stagingOrderswithAisle \
        .join(dimProduct, [stagingOrderswithAisle["PRODUCT"] == dimProduct["product_name"],
                           stagingOrderswithAisle["another_aisle_key"] == dimProduct["aisle_key"]], "leftouter") \
        .drop("PRODUCT", "AISLE", "aisle_name", "product_name", "inserted_date", "another_aisle_key", "aisles") \
        .fillna({'aisle_key': '-1', 'product_key': '-1'})\
        .withColumnRenamed("ORDER_ID", "order_key") \
        .withColumnRenamed("ADD_TO_CART_ORDER", "add_to_cart_order")

    dimOrderProductBridge.write.parquet(s3 + "/presentation_layer/dim_order_product_bridge")

def incrementalLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps:
    1. Read Current Fact Table, Staging Orders, Product Dimension and Aisle Dimension
    3. Left Anti Join, Staging Orders, Fact Table, to keep just the new orders
    2. Join filtered staging Orders with Dim_Aisle to get Aisle Key
    3. Join Resulting DataFrame with Dim Products based on product_name and
    4. Fill not matching rows
    5. Union with current dim
    6. Write results on S3
    """
    #Reading Dataset
    currentFactOrders = spark.read.parquet(s3 + "/presentation_layer/fact_order_items").select("order_key").distinct()
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")
    dimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")
    dimProduct=spark.read.parquet(s3+"presentation_layer/dim_product")

    newOrders = stagingOrders.join(currentFactOrders, stagingOrders["ORDER_ID"] == currentFactOrders["order_key"],
                                   "leftanti")

    newOrderswithAisle = newOrders.join(dimAisle, newOrders["AISLES"] == dimAisle["aisle_name"], "leftouter") \
        .withColumnRenamed("aisle_key", "another_aisle_key") \
        .drop("USER_ID", "ORDER_NUMBER", "ORDER_DOW", "ORDER_HOUR_OF_DAY", "inserted_date")

    dimOrderProductBridge = newOrderswithAisle \
        .join(dimProduct, [newOrderswithAisle["PRODUCT"] == dimProduct["product_name"],
                           newOrderswithAisle["another_aisle_key"] == dimProduct["aisle_key"]], "leftouter") \
        .drop("PRODUCT", "AISLE", "aisle_name", "product_name", "inserted_date", "another_aisle_key", "aisles") \
        .fillna({'aisle_key': '-1', 'product_key': '-1'}) \
        .withColumnRenamed("ORDER_ID", "order_key") \
        .withColumnRenamed("ADD_TO_CART_ORDER", "add_to_cart_order")
        # .show(5,False)

    dimOrderProductBridge.union(dimOrderProductBridge).write.parquet(s3 + "/tmp/dim_order_product_bridge_tmp",
                                                                     mode="overwrite")

    spark.read.parquet(s3 + "/tmp/dim_order_product_bridge_tmp") \
        .write.parquet(s3 + "/presentation_layer/dim_order_product_bridge", mode="overwrite")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('../dl.cfg')
    loadType = config.get('LOAD', 'LOADTYPE')

    s3 = config.get('S3', 'BUCKET')
    print(s3)

    # Getting Spark Session
    spark = create_spark_session()

    if loadType=="full":
        fullLoad(s3,spark)
    else:
        incrementalLoad(s3,spark)