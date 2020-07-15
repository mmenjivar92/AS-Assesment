from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import *
import datetime
import configparser


def incrementalLoad(s3,spark):
    """
    Steps
    Incremental Load:\n
    1. Read current version of dimension\n
    2. Get all the distinct values from the stagingProducts DataFrame\n
    3. Identify possible new values (left anti join with current dim)\n
    4. Generate Unique Identifier for new departmens\n
    5. Union current dimension with new values\n
    6. Write new version of the dimension to S3 Bucket => Presentation Layer
    """

    #Reading Datasets
    currentDim = spark.read.parquet(s3 + "/presentation_layer/dim_department")
    stagingProducts= spark.read.parquet(s3 + "/staging_layer/products")

    distinctDepartments = stagingProducts.select("department").distinct() \
        .withColumnRenamed("department", "department_name") \
        .withColumn("inserted_date", current_date()) \
        .select("department_name", "inserted_date")

    newDepartments=distinctDepartments.join(currentDim,distinctDepartments["department_name"]==currentDim["department_name"],"leftanti")\
        .withColumn("department_key", expr("uuid()"))\
        .select("department_key","department_name","inserted_date")

    newDim = currentDim.union(newDepartments)

    #Writting to a temp location
    newDim.write.parquet(s3 + "/tmp/dim_department_tmp", mode="overwrite")

    #Writting in final location
    spark.read.parquet(s3 + "/tmp/dim_department_tmp") \
        .write.parquet(s3 + "/presentation_layer/dim_department", mode="overwrite")


def fullLoad(s3,spark):
    """
        Steps - Incremental Load:\n
        1. Get all the distinct values from the stagingProducts DataFrame\n
        2. Generate Unique Identifier\n
        3. Write dimension to S3 Bucket => Presentation Layer\n
    """

    #Reading Datasets
    stagingProducts= spark.read.parquet(s3 + "/staging_layer/products")

    distinctDepartments = stagingProducts.select("department").distinct() \
        .withColumn("department_key", expr("uuid()")) \
        .withColumnRenamed("department", "department_name") \
        .withColumn("inserted_date", current_date()) \
        .select("department_key", "department_name", "inserted_date")

    distinctDepartments.write.parquet(s3 + "/presentation_layer/dim_department", mode="overwrite")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('../dl.cfg')
    loadType = config.get('LOAD', 'LOADTYPE')

    s3 = config.get('S3', 'BUCKET')

    # Getting Spark Session
    spark = create_spark_session()

    if loadType == "full":
        fullLoad(s3,spark)
    else:
        incrementalLoad(s3,spark)
