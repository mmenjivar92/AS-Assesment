from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import *
import configparser


def fullLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps Initial Load:\n
    1. Get all the distinct values from the stagingProducts DataFrame\n
    2. Generate Unique Identifier\n
    3. Write dimension to S3 Bucket => Presentation Layer\n
    """

    # Reading Datasets
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")

    distinctAisles = stagingProducts.select("aisle").distinct() \
        .withColumn("aisle_key", expr("uuid()")) \
        .withColumnRenamed("aisle", "aisle_name") \
        .withColumn("inserted_date", current_date()) \
        .select("aisle_key", "aisle_name", "inserted_date")

    distinctAisles.write.parquet(s3 + "/presentation_layer/dim_aisle", mode="overwrite")


def incrementalLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps:
    1. Read current version of dimension\n
    2. Get all the distinct values from the stagingProducts DataFrame\n
    3. Identify possible new values (left anti join with current dim)\n
    4. Generate Unique Identifier for new departmens\n
    5. Union current dimension with new values\n
    6. Write new version of the dimension to S3 Bucket => Presentation Layer
    """

    # Reading Datasets
    stagingProducts = spark.read.parquet(s3 + "/staging_layer/products")
    currentDimAisle = spark.read.parquet(s3 + "/presentation_layer/dim_aisle")

    distinctAisles = stagingProducts.select("aisle").distinct() \
        .withColumnRenamed("aisle", "aisle_name") \
        .withColumn("inserted_date", current_date()) \
        .select("aisle_name", "inserted_date")

    newAisles = distinctAisles.join(currentDimAisle, distinctAisles["aisle_name"] == currentDimAisle["aisle_name"],
                                    "leftanti") \
        .withColumn("aisle_key", expr("uuid()")) \
        .select("aisle_key", "aisle_name", "inserted_date")

    newDimAisles = currentDimAisle.union(newAisles)

    newDimAisles.write.parquet(s3 + "/presentation_layer/dim_aisle_tmp", mode="overwrite")

    spark.read.parquet(s3 + "/presentation_layer/dim_aisle_tmp") \
        .write.parquet(s3 + "/presentation_layer/dim_aisle", mode="overwrite")


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