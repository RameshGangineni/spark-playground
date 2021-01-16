from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType
if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('skip_first_n_rows'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------

    # Create RDD from text file
    rdd = sc.textFile('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data_modified.csv')
    print("Original Data: {}".format(rdd.collect()))
    customSchema = StructType([StructField("ID", StringType(), False),
                               StructField("NAME", StringType(), True),
                               StructField("SALARY", StringType(), True),
                               StructField("CITY", StringType(), True)]
                              )

    # Solution 1
    n = 3
    df = spark.read.csv("file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data_modified.csv", sep=',') \
        .rdd.zipWithIndex() \
        .filter(lambda x: x[1] > n) \
        .map(lambda x: x[0]).toDF(customSchema)
    print("After skipping first {} rows using zipWithIndex".format(n))
    df.show()

    # Solution 2

    df = spark.read.csv("file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/emp_data_modified.csv", sep=',', schema=customSchema)
    indexed_df = df.withColumn("idx", monotonically_increasing_id())
    filtered_df = indexed_df.filter(indexed_df['idx'] > n).drop('idx')
    print("After skipping first {} rows using monotonically_increasing_id".format(n))
    filtered_df.show()
