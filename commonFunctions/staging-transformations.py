from commonFunctions.helpers import create_spark_session
from pyspark.sql.functions import split, explode, col

import configparser


def main():
    """
    Summary:
    Entry point of the staging etl
    """

    # Reading Config file
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    s3 = config.get('HEADER', 'AWS_ACCESS_KEY_ID')

    # Getting Spark Session
    spark = create_spark_session()

    # Reading Datasets
    x = spark.read.option("header", True).csv(s3 + 'dataset.csv')
    products = spark.read.option("multiline", "true").json(s3 + "products.json")

    # Exploding Order Details
    splitedDf = x.withColumn("order_details", explode(split(col("ORDER_DETAIL"), "[~]"))).drop("ORDER_DETAIL")

    # Spliting Details into Columns
    stagingOrders = splitedDf.withColumn('PRODUCT', split(col("order_details"), "\|").getItem(0)) \
        .withColumn('AISLES', split(col("order_details"), "\|").getItem(1)) \
        .withColumn('ADD_TO_CART_ORDER', split(col("order_details"), "\|").getItem(2)) \
        .drop("order_details")

    # Parsing Products
    stagingProducts = products.select(explode("results")).select(explode("col.items")) \
        .select("col.product_name", "col.aisle", "col.department")

    # Writing Datasets in Staging partition
    stagingOrders.write.parquet(s3 + "/staging_layer/orders", mode="overwrite")
    stagingProducts.write.parquet(s3 + "/staging_layer/products", mode="overwrite")


if __name__ == "__main__":
    main()
