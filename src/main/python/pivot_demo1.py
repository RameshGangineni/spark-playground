from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('pivot_demo1'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    df = spark.read.csv('file:///home/rameshbabug/Documents/projects/internal/spark-playground/src/data/mpg.csv', header=True, inferSchema=True)
    df = df.withColumnRenamed("manufacturer", "manuf")

    # average highway mpg of cars by class and year
    df1 = df.groupBy('class', 'year').avg('hwy')
    df1.show(5)

    # Pivoting on the year column
    df2 = df.groupBy('class').pivot('year').avg('hwy')
    df2.show(5)

    # specify the values of the pivot column
    df3 = df.groupBy('class').pivot('year', [1999, 2008]).agg(F.min(df.hwy), F.max(df.hwy))
    df3.show(5)
