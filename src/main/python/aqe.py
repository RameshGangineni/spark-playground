from pyspark.sql import SparkSession
import findspark
findspark.init('/opt/spark/2.4.4/')

if __name__ == '__main__':

    # -------------------------------------------------------------------------------------------

    # Create Spark Session and Spark Context
    spark = SparkSession.\
        builder.\
        appName('AQE_Demo'). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # --------------------------------------------------------------------------------------------
    print(spark.version)
