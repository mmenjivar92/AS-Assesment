import configparser
import os
from pyspark.sql import SparkSession

#Reading Config file
config = configparser.ConfigParser()
config.read('dl.cfg')

#Getting credentials to connect to pySpark running on emr
os.environ['AWS_ACCESS_KEY_ID']=config.get('HEADER','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('HEADER','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Summary:
    Creates and return spark session

    Returns:
    spark: Spark session created.
    """
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark
