
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnyWhereData SparkETL Application") \
    .master("local") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')