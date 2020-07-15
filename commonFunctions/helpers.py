from pyspark.sql import SparkSession
import configparser

def create_spark_session():
    """
    Summary:
    Creates and return spark session

    Returns:
    spark: Spark session created.
    """

    # Reading Config file
    config = configparser.ConfigParser()
    config.read('../dl.cfg')

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['HEADER']['AWS_ACCESS_KEY_ID'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['HEADER']['AWS_SECRET_ACCESS_KEY'])
    #To run locally
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.buffer.dir", "/as-assesment-tmp")
    hadoopConf.set("fs.s3a.fast.upload", "true")
    return spark
