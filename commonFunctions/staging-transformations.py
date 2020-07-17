from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import split, explode, col

import configparser


def main():
    """
    Summary:
    Staging ETL
    """

    # Reading Config file
    config = configparser.ConfigParser()
    config.read('../dl.cfg')
    s3 = config.get('S3', 'BUCKET')

    # Getting Spark Session
    spark = create_spark_session()

    print("Starting Staging Process")

    # Reading Datasets
    print("Reading Datasets")
    x= x= spark.read.option("header",True).option("encoding", "cp1252").option("sep", ",").option("escape", "\"").csv(s3+"/raw_layer/dataset.csv")
    products = spark.read.option("multiline", "true").json(s3+"/raw_layer/products.json")

    # Exploding Order Details
    print("Exploding Order Details")
    splitedDf = x.withColumn("order_details", explode(split(col("ORDER_DETAIL"), "[~]"))).drop("ORDER_DETAIL")

    # Spliting Details into Columns
    print("Spliting Details into Columns")
    stagingOrders = splitedDf.withColumn('PRODUCT', split(col("order_details"), "\|").getItem(0)) \
        .withColumn('AISLES', split(col("order_details"), "\|").getItem(1)) \
        .withColumn('ADD_TO_CART_ORDER', split(col("order_details"), "\|").getItem(2)) \
        .drop("order_details")

    # Parsing Products
    print("Parsing Products")
    stagingProducts = products.select(explode("results")).select(explode("col.items")) \
        .select("col.product_name", "col.aisle", "col.department")

    # Writing Datasets in Staging partition
    print("Writing Datasets in Staging partition")
    stagingOrders.write.parquet(s3 + "/staging_layer/orders", mode="overwrite")
    stagingProducts.write.parquet(s3 + "/staging_layer/products", mode="overwrite")


if __name__ == "__main__":
    main()
