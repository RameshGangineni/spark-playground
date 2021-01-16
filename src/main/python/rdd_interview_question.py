from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def cus_udf(row):
    print(row[1][0])
    if 's' in row[1][0].lower():
        row[1] = row[1] + "-checked"
        row[2] = None
    return row


spark = SparkSession. \
    builder. \
    appName('rdd'). \
    enableHiveSupport(). \
    getOrCreate()

sc = spark.sparkContext
rdd = sc.textFile('/home/rameshbabug/Documents/projects/internal/spark-playground/src/data/rdd_data.txt')
spark.udf.register("cus_udf", cus_udf, StringType())
rdd1 = rdd.filter(lambda row: 'name' not in row).map(lambda row: row.split(',')).map(lambda x: cus_udf(x))
print(rdd1.collect())
