from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Visualize").getOrCreate()
code = ['GIL', 'LAF', 'KDC', 'FPT']

hdfs_csv_path = "hdfs://node01:9000/stock1/GIL.csv"

df = spark.read.csv(hdfs_csv_path, header=True, inferSchema=True)
df.printSchema()
df.show()

spark.stop()