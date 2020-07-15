from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import *
import configparser


def fullLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Initial Load:
    1. Get Distinct Products from stagingProducts DataFrame
    2. Generate unique identifier
    2. Read Aisles Dim to get the keys
    3. Join UniqueProducts with Aisles Dims
    4. Filling not match records whit -1 Key (Undefined)
    5. Write results in S3
    """
    # Reading Datasets
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")
    dimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")

    stagingDimProducts=stagingProducts.drop("department").distinct()\
        .withColumn("product_key", expr("uuid()"))

    DimProducts = stagingDimProducts.join(dimAisle, stagingDimProducts["aisle"] == dimAisle["aisle_name"], "left") \
        .drop("aisle", "aisle_name", "inserted_date") \
        .withColumn("inserted_date", current_date()) \
        .fillna({'aisle_key': '-1'}) \
        .select("product_key", "aisle_key", "product_name", "inserted_date")

    DimProducts.write.parquet(s3 + "/presentation_layer/dim_product", mode="overwrite")


def incrementalLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session
    0. Read Current Dim Products
    1. Identify new Products (AntiJoin currentDimProducts with new stagingProducts)
    2. Get Distinct Products from new Products DataFrame
    2. Generate unique identifier
    2. Read Aisles Dim to get the keys
    3. Join UniqueProducts with Aisles Dims
    4. Filling not match records whit -1 Key (Undefined)
    5. Union currentDimProducts with newProducts
    6. Write results in S3
    """
    #Reading Datasets
    currentDimProducts = spark.read.parquet(s3 + "/presentation_layer/dim_product")
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")
    dimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")

    newProducts = stagingProducts.join(currentDimProducts,
                                       stagingProducts["product_name"] == currentDimProducts["product_name"],
                                       "left_anti")

    stagingNewProducts = newProducts.drop("department").distinct() \
        .withColumn("product_key", expr("uuid()"))

    newDimProducts=stagingNewProducts.join(dimAisle,stagingNewProducts["aisle"]==dimAisle["aisle_name"],"left")\
        .drop("aisle","aisle_name","inserted_date")\
        .withColumn("inserted_date",current_date())\
        .fillna({'aisle_key':'-1'})\
        .select("product_key","aisle_key","product_name","inserted_date").union(currentDimProducts)

    newDimProducts.write.parquet(s3+"/presentation_layer/dim_products_tmp",mode="overwrite")

    spark.read.parquet(s3+"/presentation_layer/dim_products_tmp")\
        .write.parquet(s3+"/presentation_layer/dim_products",mode="overwrite")


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