from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import *
import configparser


def fullLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Initial Load:
    1. Get all the distinct values from the stagingOrders DataFrame
    3. Write dimension to S3 Bucket => Presentation Layer
    """
    # Reading Datasets
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")

    distinctUsers = stagingOrders.select("USER_ID").distinct() \
        .withColumn("inserted_date", current_date()) \
        .withColumnRenamed("USER_ID", "user_key") \
        .select("user_key", "inserted_date")

    distinctUsers.write.parquet(s3 + "/presentation_layer/dim_user", mode="overwrite")


def incrementalLoad(s3,spark):
    """
    Inputs:
    s3: S3 Bucket Address
    spark: Spark Session

    Steps:
    1. Read current version of dimension
    2. Get all the distinct values from the stagingOrders DataFrame
    3. Identify possible new values (left anti join with current dim)
    5. Union current dimension with new values
    6. Write new version of the dimension to S3 Bucket => Presentation Layer
    """
    #Reading Datasets
    currentDimUser = spark.read.parquet(s3 + "/presentation_layer/dim_user")
    stagingOrders = spark.read.parquet(s3 + "/staging_layer/orders")

    distinctUsers=stagingOrders.select("USER_ID").distinct()\
        .withColumn("inserted_date",current_date())\
        .withColumnRenamed("USER_ID","user_key")\
        .select("user_key","inserted_date")

    newUsers = distinctUsers.join(currentDimUser, distinctUsers["user_key"] == currentDimUser["user_key"], "leftanti")

    newDimUser = currentDimUser.union(newUsers)

    newDimUser.write.parquet(s3 + "/tmp/dim_user_tmp", mode="overwrite")

    spark.read.parquet(s3 + "/tmp/dim_user_tmp") \
        .write.parquet(s3 + "/presentation_layer/dim_user", mode="overwrite")

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